"""
control_panel/app.py
======================
The S1Wave control panel (2026-09-29) — a private homepage: log in, see
every trading account at a glance, create a new one. Explicitly scoped
to ONE owner (see /signup below) after the user ruled out a public,
multi-tenant version — real custody/compliance questions that don't
apply to a private admin tool for someone's own accounts.

Runs as its OWN process (control_panel_main.py), its own database
(s1wave_control, via .env.control's DATABASE_URL), separate from every
trading account it lists. Never touches a trading account's wallet key
or database directly — only ever talks to a trading account's own API
over HTTP (the exact same way a browser hitting that account's own
dashboard would), using the Basic Auth credentials captured in the
registry at account-creation time.

Mounted at /panel (2026-09-29) — served through the SAME domain as the
base account's own dashboard (https://s1wave-solana.duckdns.org/), not a
new subdomain, per the user's explicit request. Every route below is
under PREFIX and every internal redirect/form-action string is written
with that prefix explicitly (NOT relying on nginx to strip/rewrite paths)
— the base dashboard's own "/" route already occupies the domain root, so
this app has to know its own mount point itself rather than assume it
owns "/". See nginx's active.conf for the matching `location /panel/`
block (passes the full path through unchanged, no prefix-stripping).
"""

from __future__ import annotations

import asyncio

import bcrypt
import httpx
from fastapi import APIRouter, FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import func, select
from starlette.middleware.sessions import SessionMiddleware

from config.logging import get_logger
from config.settings import settings
from control_panel.models import Base, ControlAccount, ControlUser
from database.engine import get_engine, get_session
from engine.provisioning import (
    TRADING_PARAMS,
    ProvisioningError,
    create_account,
    generate_dashboard_credentials,
    get_solana_tracker_keys,
    get_trading_params,
    public_url_for,
    update_dashboard_credentials,
    update_solana_tracker_keys,
    update_trading_params,
    validate_trading_param,
)

log = get_logger(__name__)

PREFIX = "/panel"


def _dashboard_link_for(acct: ControlAccount) -> str | None:
    """An account's public URL, but ONLY once nginx actually has a working
    route for it (2026-09-29) — every account gets the same URL shape
    (public_url_for), but add_nginx_route() runs as a best-effort last
    step of account creation and can land False. Showing the link anyway
    would just be a dead link with an extra, more confusing 502/504 step."""
    return public_url_for(acct.name) if acct.nginx_configured else None

app = FastAPI(title="S1Wave Control Panel")

if not settings.CONTROL_SESSION_SECRET:
    raise RuntimeError(
        "CONTROL_SESSION_SECRET is not set in .env.control — refusing to start with an "
        "insecure default session signing key. Generate one (e.g. `python -c "
        "\"import secrets; print(secrets.token_hex(32))\"`) and set it before starting this service."
    )
app.add_middleware(SessionMiddleware, secret_key=settings.CONTROL_SESSION_SECRET, https_only=True)

router = APIRouter(prefix=PREFIX)


@app.on_event("startup")
async def _on_startup() -> None:
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("control_panel.started", port=settings.API_PORT)


# Design tokens adopted from the AnchorLedger web app (2026-10-01, per the
# user's explicit request) — same palette/typography as that project's
# frontend/src/lib/tokens.js, translated into plain CSS custom properties
# since this app is server-rendered HTML, not React. Deliberately the SAME
# values, not "inspired by" — green/red/violet/orange carry the exact
# meanings AnchorLedger's NotificationCenter/status maps use them for.
_TOKENS_CSS = """
  :root {
    --bg:#0A0A0F; --surface:#111118; --surface2:#16161F;
    --border:#1E1E2E; --border2:#252538;
    --text:#E8F4F8; --muted:#5A6478;
    --green:#00D4AA; --red:#FF4D6D; --violet:#7B61FF; --orange:#FF8C00;
    /* Back-compat aliases so every existing .card/.pill/form rule below
       keeps working unchanged under the new palette. */
    --accent:#00D4AA; --danger:#FF4D6D; --warn:#FF8C00; --bright:#E8F4F8;
  }
"""

_NAV_ITEMS = [("/", "▦", "Accounts"), ("/playbook", "📘", "Playbook")]


def _notification_bell_html() -> str:
    return f"""
      <div class="notif-wrap" id="notifWrap">
        <button class="notif-bell" id="notifBell" onclick="s1wToggleNotifs()">
          🔔<span class="notif-badge" id="notifBadge" hidden>0</span>
        </button>
        <div class="notif-panel" id="notifPanel" hidden>
          <div class="notif-header">Notifications</div>
          <div class="notif-list" id="notifList">
            <div class="notif-empty">Loading…</div>
          </div>
        </div>
      </div>
      <script>
        (function() {{
          const PREFIX = {PREFIX!r};
          const LAST_SEEN_KEY = 's1wave_notif_last_seen';
          const LEVEL_COLOR = {{critical:'var(--red)', warning:'var(--orange)', info:'var(--green)'}};

          function timeAgo(iso) {{
            const diff = (Date.now() - new Date(iso).getTime()) / 1000;
            if (diff < 60) return Math.floor(diff) + 's ago';
            if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
            if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
            return Math.floor(diff / 86400) + 'd ago';
          }}

          function render(items) {{
            let lastSeen = '1970-01-01T00:00:00Z';
            try {{ lastSeen = localStorage.getItem(LAST_SEEN_KEY) || lastSeen; }} catch (e) {{}}
            const unread = items.filter(n => n.created_at > lastSeen).length;
            const badge = document.getElementById('notifBadge');
            badge.hidden = unread === 0;
            badge.textContent = unread > 99 ? '99+' : String(unread);

            const list = document.getElementById('notifList');
            if (!items.length) {{
              list.innerHTML = '<div class="notif-empty">No notifications</div>';
              return;
            }}
            list.innerHTML = items.map(n => `
              <div class="notif-item">
                <div class="notif-dot" style="background:${{LEVEL_COLOR[n.level] || 'var(--muted)'}}"></div>
                <div class="notif-body">
                  <div class="notif-title">[${{n.account}}] ${{n.event.replace(/_/g, ' ')}}</div>
                  <div class="notif-msg">${{n.message}}</div>
                  <div class="notif-time">${{timeAgo(n.created_at)}}</div>
                </div>
              </div>
            `).join('');
          }}

          async function poll() {{
            try {{
              const res = await fetch(PREFIX + '/api/notifications');
              if (!res.ok) return;
              render(await res.json());
            }} catch (e) {{}}
          }}

          window.s1wToggleNotifs = function() {{
            const panel = document.getElementById('notifPanel');
            panel.hidden = !panel.hidden;
            if (!panel.hidden) {{
              poll();
              try {{ localStorage.setItem(LAST_SEEN_KEY, new Date().toISOString()); }} catch (e) {{}}
              setTimeout(poll, 300); // re-render badge cleared after marking seen
            }}
          }};

          document.addEventListener('click', function(e) {{
            const wrap = document.getElementById('notifWrap');
            if (wrap && !wrap.contains(e.target)) document.getElementById('notifPanel').hidden = true;
          }});

          poll();
          setInterval(poll, 30000);
        }})();
      </script>"""


def _layout(title: str, body: str, show_chrome: bool = True) -> str:
    nav_html = ""
    if show_chrome:
        nav_links = "".join(
            f'<a href="{PREFIX}{path}" class="nav-link">{icon} {label}</a>'
            for path, icon, label in _NAV_ITEMS
        )
        nav_html = f"""
        <div class="appbar">
          <div class="appbar-brand">S1WAVE</div>
          <nav class="appbar-nav">{nav_links}</nav>
          {_notification_bell_html()}
        </div>"""

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>{title} — S1Wave</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
{_TOKENS_CSS}
  * {{ box-sizing:border-box; }}
  body {{ background:var(--bg); color:var(--text); font-family:'Inter',system-ui,sans-serif;
          margin:0; min-height:100vh; }}
  code, .mono {{ font-family:'JetBrains Mono',monospace; }}
  .wrap {{ max-width:720px; margin:0 auto; padding:24px 16px 48px; }}
  h1 {{ color:var(--bright); font-size:22px; margin:0 0 4px; }}
  .sub {{ color:var(--muted); font-size:13px; margin-bottom:28px; }}
  .card {{ background:var(--surface); border:1px solid var(--border); border-radius:10px;
           padding:20px; margin-bottom:16px; }}
  label {{ display:block; font-family:'JetBrains Mono',monospace; font-size:10px; letter-spacing:0.06em;
           text-transform:uppercase; color:var(--muted); margin-bottom:6px; margin-top:14px; }}
  input, select {{ width:100%; background:var(--bg); border:1px solid var(--border2); border-radius:6px;
           color:var(--bright); padding:10px 12px; font-size:14px; font-family:inherit; }}
  input:focus, select:focus {{ outline:none; border-color:var(--green) !important; }}
  button, .btn {{ background:var(--green); color:#04221c; border:none; border-radius:6px;
           padding:11px 18px; font-weight:700; font-size:14px; cursor:pointer; margin-top:18px;
           display:inline-block; text-decoration:none; }}
  button:hover, .btn:hover {{ filter:brightness(1.1); }}
  .btn-secondary {{ background:transparent; border:1px solid var(--border2); color:var(--text); }}
  a {{ color:var(--green); }}
  .error {{ color:var(--red); font-size:13px; margin-top:10px; }}
  .account-row {{ display:flex; justify-content:space-between; align-items:center;
           padding:14px 0; border-bottom:1px solid var(--border); gap:12px; flex-wrap:wrap; }}
  .account-row:last-child {{ border-bottom:none; }}
  .account-name {{ font-weight:700; color:var(--bright); font-family:'JetBrains Mono',monospace; }}
  .account-meta {{ font-size:12px; color:var(--muted); }}
  .pill {{ font-family:'JetBrains Mono',monospace; font-size:10px; letter-spacing:0.04em;
           padding:3px 9px; border-radius:999px; font-weight:700; text-transform:uppercase; }}
  .pill.armed {{ background:rgba(0,212,170,.15); color:var(--green); }}
  .pill.paused {{ background:rgba(255,140,0,.15); color:var(--orange); }}
  .pill.halted {{ background:rgba(255,77,109,.15); color:var(--red); }}
  .pill.unknown {{ background:rgba(90,100,120,.2); color:var(--muted); }}
  .topbar {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:24px; }}
  .topbar a {{ color:var(--muted); font-size:13px; text-decoration:none; }}

  /* App chrome — persistent nav + notification bell (2026-10-01) */
  .appbar {{ display:flex; align-items:center; gap:18px; padding:12px 16px;
             background:var(--surface); border-bottom:1px solid var(--border);
             position:sticky; top:0; z-index:200; }}
  .appbar-brand {{ font-family:'JetBrains Mono',monospace; font-weight:700; font-size:13px;
                   letter-spacing:0.08em; color:var(--green); }}
  .appbar-nav {{ display:flex; gap:4px; flex:1; }}
  .nav-link {{ font-family:'JetBrains Mono',monospace; font-size:12px; color:var(--muted);
               text-decoration:none; padding:6px 10px; border-radius:6px; }}
  .nav-link:hover {{ background:var(--surface2); color:var(--text); }}

  .notif-wrap {{ position:relative; }}
  .notif-bell {{ background:transparent; border:1px solid var(--border2); border-radius:6px;
                 padding:6px 10px; margin:0; font-size:14px; position:relative; color:var(--muted); }}
  .notif-bell:hover {{ filter:none; border-color:var(--green); color:var(--green); }}
  .notif-badge {{ position:absolute; top:-6px; right:-6px; background:var(--red); color:#fff;
                  font-family:'JetBrains Mono',monospace; font-size:9px; font-weight:700;
                  padding:1px 5px; border-radius:8px; min-width:16px; text-align:center; }}
  .notif-panel {{ position:absolute; top:40px; right:0; width:min(360px, 90vw); max-height:420px;
                  background:var(--surface); border:1px solid var(--border2); border-radius:8px;
                  box-shadow:0 8px 32px rgba(0,0,0,.6); overflow-y:auto; z-index:300; }}
  .notif-header {{ padding:12px 16px; font-family:'JetBrains Mono',monospace; font-size:11px;
                    font-weight:700; letter-spacing:0.04em; text-transform:uppercase;
                    color:var(--muted); border-bottom:1px solid var(--border); }}
  .notif-empty {{ padding:24px 16px; text-align:center; font-size:12px; color:var(--muted); }}
  .notif-item {{ display:flex; gap:10px; padding:12px 16px; border-bottom:1px solid var(--border); }}
  .notif-item:last-child {{ border-bottom:none; }}
  .notif-dot {{ width:7px; height:7px; border-radius:50%; margin-top:5px; flex-shrink:0; }}
  .notif-title {{ font-family:'JetBrains Mono',monospace; font-size:11px; font-weight:600;
                  color:var(--text); text-transform:uppercase; letter-spacing:0.02em; }}
  .notif-msg {{ font-size:12px; color:var(--muted); margin-top:3px; line-height:1.4; }}
  .notif-time {{ font-family:'JetBrains Mono',monospace; font-size:10px; color:var(--muted); margin-top:4px; }}
</style></head>
<body>{nav_html}<div class="wrap">{body}</div></body></html>"""


def _require_login(request: Request) -> str | None:
    return request.session.get("user")


@router.get("/signup", response_class=HTMLResponse)
async def signup_form(request: Request):
    async with get_session() as session:
        count = (await session.execute(select(func.count()).select_from(ControlUser))).scalar_one()
    if count > 0:
        return RedirectResponse(f"{PREFIX}/login", status_code=303)
    return HTMLResponse(_layout("Sign up", f"""
        <h1>Create your S1Wave login</h1>
        <div class="sub">One-time setup — this becomes the only login for this control panel.</div>
        <div class="card">
        <form method="post" action="{PREFIX}/signup">
            <label>Username</label><input name="username" required autofocus>
            <label>Password</label><input name="password" type="password" required minlength="8">
            <button type="submit">Create account</button>
        </form>
        </div>
    """, show_chrome=False))


@router.post("/signup")
async def signup_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    # Strip whitespace (2026-09-29, real bug: a browser autofill/paste left
    # a trailing space in the stored username that made login impossible
    # since no one would ever type that same trailing space back).
    username = username.strip()
    async with get_session() as session:
        count = (await session.execute(select(func.count()).select_from(ControlUser))).scalar_one()
        if count > 0:
            return RedirectResponse(f"{PREFIX}/login", status_code=303)
        password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        session.add(ControlUser(username=username, password_hash=password_hash))
    request.session["user"] = username
    return RedirectResponse(PREFIX + "/", status_code=303)


@router.get("/login", response_class=HTMLResponse)
async def login_form(request: Request, error: str | None = None):
    async with get_session() as session:
        count = (await session.execute(select(func.count()).select_from(ControlUser))).scalar_one()
    if count == 0:
        return RedirectResponse(f"{PREFIX}/signup", status_code=303)
    error_html = f'<div class="error">{error}</div>' if error else ""
    return HTMLResponse(_layout("Log in", f"""
        <h1>S1Wave Control Panel</h1>
        <div class="sub">Log in to manage your trading accounts.</div>
        <div class="card">
        <form method="post" action="{PREFIX}/login">
            <label>Username</label><input name="username" required autofocus>
            <label>Password</label><input name="password" type="password" required>
            <button type="submit">Log in</button>
            {error_html}
        </form>
        </div>
    """, show_chrome=False))


@router.post("/login")
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    # Case-insensitive, whitespace-tolerant username match (2026-09-29,
    # real bug: a real login attempt failed because of both a case
    # mismatch AND a trailing space stored from signup — "Username" fields
    # being silently case/whitespace-sensitive is a classic, avoidable
    # source of "I can't log in").
    username = username.strip()
    async with get_session() as session:
        user = (await session.execute(
            select(ControlUser).where(func.lower(func.trim(ControlUser.username)) == username.lower())
        )).scalar_one_or_none()
    if user is None or not bcrypt.checkpw(password.encode(), user.password_hash.encode()):
        return RedirectResponse(f"{PREFIX}/login?error=Invalid+username+or+password", status_code=303)
    request.session["user"] = user.username
    return RedirectResponse(PREFIX + "/", status_code=303)


@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(f"{PREFIX}/login", status_code=303)


@router.get("/", response_class=HTMLResponse)
async def homepage(request: Request):
    """
    Account list (2026-09-29: deliberately NO balance/status/financial
    info here anymore — see the user's explicit instruction). This is
    the login-gated ACCOUNT SWITCHER, not a financial summary; each
    account's own dashboard (linked from its manage page) is where real
    numbers belong, behind that account's own Basic Auth. Identity only:
    name, a link to manage it (SolanaTracker keys, etc.), and a link to
    its live dashboard where one is public.
    """
    user = _require_login(request)
    if not user:
        return RedirectResponse(f"{PREFIX}/login", status_code=303)

    async with get_session() as session:
        accounts = (await session.execute(
            select(ControlAccount).order_by(ControlAccount.created_at)
        )).scalars().all()

    rows = ""
    for acct in accounts:
        public_url = _dashboard_link_for(acct)
        open_link = (f'<a class="btn btn-secondary" href="{public_url}" target="_blank">Open dashboard</a>'
                     if public_url else '<span class="account-meta">no public dashboard yet</span>')
        rows += f"""
        <div class="account-row">
            <div class="account-name">{acct.name}</div>
            <div style="display:flex;align-items:center;gap:10px">
                {open_link}
                <a class="btn btn-secondary" href="{PREFIX}/accounts/{acct.name}">Manage</a>
                <a class="btn btn-secondary" href="{PREFIX}/accounts/{acct.name}/settings">⚙ Settings</a>
            </div>
        </div>"""
    if not rows:
        rows = '<div class="account-meta">No accounts yet.</div>'

    return HTMLResponse(_layout("Accounts", f"""
        <div class="topbar">
            <h1 style="margin:0">S1Wave Accounts</h1>
            <a href="{PREFIX}/logout">Log out ({user})</a>
        </div>
        <div class="card">{rows}</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
            <a class="btn" href="{PREFIX}/accounts/new">+ New Account</a>
            <a class="btn btn-secondary" href="{PREFIX}/playbook">📘 Playbook — what to do when...</a>
        </div>
    """))


@router.get("/playbook", response_class=HTMLResponse)
async def playbook(request: Request):
    """
    Plain-language operator runbook (2026-09-30) — built the day the
    user's Claude subscription was about to lapse and they'd be running
    this alone. Written FOR THE HUMAN OPERATOR, not for an AI picking up
    the codebase — CLAUDE.md's "Resuming after a blind period" section
    already covers that audience. This is the one page that should never
    assume a terminal, SSH access, or reading source code is available.
    """
    if not _require_login(request):
        return RedirectResponse(f"{PREFIX}/login", status_code=303)

    def section(title, body):
        return f'<div class="card"><div class="sub" style="margin-bottom:8px;font-size:15px;color:var(--bright)">{title}</div>{body}</div>'

    body = f"""
        <div class="topbar">
            <h1 style="margin:0">Playbook</h1>
            <a href="{PREFIX}/">&larr; All accounts</a>
        </div>
        <div class="sub">What to actually do, in plain terms, for the situations that come up.
        Every action below is a page in this app — nothing here needs a terminal.</div>

        {section("No new trades happening / bot seems idle", '''
            <p>Open the account's own dashboard (Manage → the link near the top) and check the live feed.
            The single most common cause: <b>the SolanaTracker sampling key ran out</b> — these are
            free-tier keys with a lifetime cap, not a daily one, and once it's spent it's spent for good.</p>
            <p><b>Fix:</b> get a fresh key from <a href="https://solanatracker.io" target="_blank">solanatracker.io</a>
            (sign up, generate an API key), then go to Settings → SolanaTracker keys → paste it into the
            <i>Sampling key</i> field → Save. The account restarts automatically and should start seeing
            real data again within a minute — you'll know it worked when Telegram goes quiet on errors and
            you start seeing normal activity again.</p>
            <p>Discovery uses a SEPARATE key from sampling — if only ONE of the two is dead, replace just
            that one field and leave the other alone.</p>
        ''')}

        {section("You got a 'circuit breaker tripped' Telegram alert", '''
            <p>This means 3 real trades lost money in a row, and the bot has automatically PAUSED new
            entries for 60 minutes (both numbers are adjustable — see below). This is a safety feature
            working correctly, not an error.</p>
            <p><b>Do nothing</b> and it resumes automatically once the cooldown passes (you'll get a
            "circuit breaker resumed" alert). If you want to investigate first, use the Playbook section
            below on checking recent trades, or just leave it — the pause itself is the protection.</p>
            <p>If this is happening too often or not often enough, go to Settings → Trading parameters and
            adjust <i>"losses in a row before pausing"</i> or <i>"pause duration"</i>.</p>
        ''')}

        {section("You want to reduce or increase exposure", '''
            <p>Go to the account's Settings page → <b>Trading parameters</b>. The two numbers that matter most:</p>
            <ul style="margin:8px 0;padding-left:20px;color:var(--text)">
                <li><b>Exposure %</b> — the fraction of your wallet balance that can be tied up in open
                positions at once. Lower = smaller bets, survives a bad stretch longer. This is usually
                the number to change first.</li>
                <li><b>Max concurrent positions</b> — how many trades can be open simultaneously. Lower
                also limits how much damage a single burst of correlated bad signals can do before you
                or the circuit breaker can react.</li>
            </ul>
            <p>As capital grows, raise <i>Max position size (USD)</i> too — that's a hard ceiling per
            trade that doesn't move with exposure %.</p>
            <p>Every change here restarts the account — takes a few seconds, doesn't touch open positions.</p>
        ''')}

        {section("You need to stop everything RIGHT NOW", '''
            <p>Settings → Trading parameters → <b>"Live trading enabled"</b> → set to <b>False</b> → Save.
            This stops all NEW entries immediately. Anything already open keeps being monitored and can
            still exit normally (stop-loss, take-profit, etc.) — this never abandons a position with money
            on the line.</p>
            <p>The account's own dashboard also has a Pause/Resume toggle that does the same thing, if
            you're already looking at it.</p>
        ''')}

        {section("You want to take profit out / withdraw", '''
            <p>Go to the account's OWN dashboard (not this panel) → the "Withdraw funds" card near the
            bottom. Enter an amount and a destination address. <b>This is real, on-chain, and
            irreversible</b> — double-check the address before confirming, there is no undo.</p>
        ''')}

        {section("Understanding what you'll see on Telegram", '''
            <p>You get a push for: entries and exits on every real trade, a permanent halt, a token marked
            unsellable, a critical sell failure, a daily loss limit hit, a withdrawal going out, a filter
            calibration warning, and (as of today) circuit breaker trips/resumes. Everything else stays
            in-app only (visible on the dashboard) to avoid flooding your phone.</p>
        ''')}

        {section("Setting up a new account", '''
            <p>Homepage → <b>+ New Account</b>. Comes up paused and unfunded on purpose — fund the wallet
            address it gives you, add its own SolanaTracker keys (Settings page), then flip
            "Live trading enabled" to True when you're ready. Every account is fully isolated: its own
            database, wallet, keys, and dashboard login — nothing you do to one touches another.</p>
        ''')}

        {section("Checking on things without this panel", '''
            <p>Each account's own dashboard (linked from its Manage page) shows live status, open positions,
            trade history, and P&amp;L directly — Basic Auth protected, credentials visible on that
            account's Settings page here if you've forgotten them.</p>
        ''')}
    """
    return HTMLResponse(_layout("Playbook", body))


@router.get("/accounts/new", response_class=HTMLResponse)
async def new_account_form(request: Request, error: str | None = None):
    if not _require_login(request):
        return RedirectResponse(f"{PREFIX}/login", status_code=303)
    error_html = f'<div class="error">{error}</div>' if error else ""
    return HTMLResponse(_layout("New account", f"""
        <h1>New S1Wave account</h1>
        <div class="sub">Creates an isolated database, a fresh wallet, its own port and service.
        Comes up paused and unfunded — you fund the wallet and flip it on when ready.</div>
        <div class="card">
        <form method="post" action="{PREFIX}/accounts/new">
            <label>Account name (lowercase, digits, hyphens)</label>
            <input name="name" required pattern="[a-z0-9-]+" autofocus placeholder="e.g. trading2">
            <button type="submit">Create</button>
            {error_html}
        </form>
        </div>
        <a href="{PREFIX}/">&larr; Back</a>
    """))


@router.post("/accounts/new")
async def new_account_submit(request: Request, name: str = Form(...)):
    if not _require_login(request):
        return RedirectResponse(f"{PREFIX}/login", status_code=303)
    try:
        await create_account(name)
    except ProvisioningError as exc:
        return RedirectResponse(f"{PREFIX}/accounts/new?error={exc}", status_code=303)
    return RedirectResponse(PREFIX + "/", status_code=303)


def _trading_param_field(key: str, spec: dict, value: str) -> str:
    if spec["type"] == "bool":
        checked_true = "selected" if value.strip().lower() == "true" else ""
        checked_false = "selected" if value.strip().lower() == "false" else ""
        input_html = (f'<select name="{key}">'
                      f'<option value="True" {checked_true}>True (armed)</option>'
                      f'<option value="False" {checked_false}>False (paused)</option>'
                      f'</select>')
    else:
        step = "any" if spec["type"] == "float" else "1"
        bounds = ""
        if "min" in spec:
            bounds += f' min="{spec["min"]}"'
        if "max" in spec:
            bounds += f' max="{spec["max"]}"'
        input_html = f'<input type="number" step="{step}" name="{key}" value="{value}"{bounds}>'
    return f"""
        <label>{spec['label']}</label>
        {input_html}
        <div class="account-meta" style="margin-top:4px">{spec['help']}</div>"""


async def _load_account_or_redirect(name: str) -> ControlAccount | RedirectResponse:
    async with get_session() as session:
        acct = (await session.execute(
            select(ControlAccount).where(ControlAccount.name == name)
        )).scalar_one_or_none()
    return acct if acct is not None else RedirectResponse(PREFIX + "/", status_code=303)


@router.get("/accounts/{name}", response_class=HTMLResponse)
async def manage_account(request: Request, name: str):
    """
    Per-account overview (2026-09-29, split from Settings on 2026-10-01 —
    the user asked for a dedicated settings page per account once there
    was enough to tune that cramming it alongside identity/wallet info
    stopped making sense). Identity only, no balance/financial data (that
    stays on the account's OWN dashboard, behind its own Basic Auth; see
    the homepage's docstring for why) — everything editable lives at
    /accounts/{name}/settings instead.
    """
    if not _require_login(request):
        return RedirectResponse(f"{PREFIX}/login", status_code=303)

    acct = await _load_account_or_redirect(name)
    if isinstance(acct, RedirectResponse):
        return acct

    public_url = _dashboard_link_for(acct)
    dashboard_link = (f'<a href="{public_url}" target="_blank">{public_url}</a>' if public_url
                       else "no public dashboard yet — reachable on its own port on this server")

    return HTMLResponse(_layout(name, f"""
        <div class="topbar">
            <h1 style="margin:0">{name}</h1>
            <a href="{PREFIX}/">&larr; All accounts</a>
        </div>
        <div class="card">
            <label>Wallet address</label>
            <div class="account-meta" style="word-break:break-all">{acct.wallet_pubkey}</div>
            <label>Dashboard</label>
            <div class="account-meta">{dashboard_link}</div>
        </div>
        <a class="btn" href="{PREFIX}/accounts/{name}/settings">⚙ Settings</a>
    """))


@router.get("/accounts/{name}/settings", response_class=HTMLResponse)
async def account_settings(
    request: Request, name: str, saved: str | None = None, error: str | None = None,
    for_: str | None = Query(default=None, alias="for"),
):
    """
    Everything tunable for one account, in one place (2026-10-01) — trading
    risk parameters, SolanaTracker keys, dashboard credentials. Built the
    day the user explicitly asked for "a settings page in each account I
    can use to tune all these things," after a session spent reducing
    exposure and building the circuit breaker by hand through me — the
    whole point from here is that none of that should ever need an AI or a
    terminal again.
    """
    if not _require_login(request):
        return RedirectResponse(f"{PREFIX}/login", status_code=303)

    acct = await _load_account_or_redirect(name)
    if isinstance(acct, RedirectResponse):
        return acct

    try:
        keys = get_solana_tracker_keys(name)
    except ProvisioningError:
        keys = {"sampling": "", "discovery": ""}
    try:
        trading_params = get_trading_params(name)
    except ProvisioningError:
        trading_params = {key: spec["default"] for key, spec in TRADING_PARAMS.items()}

    saved_html = '<div class="sub" style="color:var(--accent)">Saved — the account restarted with the new keys.</div>' if saved == "keys" else ""
    creds_saved_html = ('<div class="sub" style="color:var(--accent)">Password regenerated — the account restarted. '
                         'Copy it now, it won\'t be shown differently again.</div>') if saved == "creds" else ""
    params_saved_html = '<div class="sub" style="color:var(--accent)">Saved — the account restarted with the new parameters.</div>' if saved == "params" else ""
    error_html = f'<div class="error">{error}</div>' if error and for_ != "params" else ""
    params_error_html = f'<div class="error">{error}</div>' if error and for_ == "params" else ""

    return HTMLResponse(_layout(f"{name} — Settings", f"""
        <div class="topbar">
            <h1 style="margin:0">{name} — Settings</h1>
            <a href="{PREFIX}/accounts/{name}">&larr; {name}</a>
        </div>
        <div class="card">
            <div class="sub" style="margin-bottom:0">Trading parameters</div>
            <div class="account-meta">Tune this account's own risk/exposure controls directly — no
            server access needed. Saving restarts this account's service to apply them.</div>
            <form method="post" action="{PREFIX}/accounts/{name}/trading-params">
                {"".join(_trading_param_field(k, spec, trading_params[k]) for k, spec in TRADING_PARAMS.items())}
                <button type="submit" style="margin-top:18px">Save &amp; restart account</button>
                {params_saved_html}
                {params_error_html}
            </form>
        </div>
        <div class="card">
            <div class="sub" style="margin-bottom:0">SolanaTracker keys</div>
            <div class="account-meta">Add or replace this account's own discovery/sampling keys.
            Saving restarts this account's service to apply them — doesn't touch any other account.</div>
            <form method="post" action="{PREFIX}/accounts/{name}/keys">
                <label>Sampling key (SOLANA_TRACKER_API_KEY)</label>
                <input name="sampling_key" value="{keys['sampling']}" placeholder="leave blank to clear">
                <label>Discovery key (SOLANA_TRACKER_API_KEY_DISCOVERY)</label>
                <input name="discovery_key" value="{keys['discovery']}" placeholder="leave blank to clear">
                <button type="submit">Save &amp; restart account</button>
                {saved_html}
                {error_html}
            </form>
        </div>
        <div class="card">
            <div class="sub" style="margin-bottom:0">Dashboard login (Basic Auth)</div>
            <div class="account-meta">This is what a browser asks for when opening this account's own
            dashboard — distinct per account, so one account's login can't open another's.</div>
            <label>Username</label>
            <input value="{acct.dashboard_auth_user}" readonly>
            <label>Password</label>
            <input value="{acct.dashboard_auth_password}" readonly>
            <form method="post" action="{PREFIX}/accounts/{name}/dashboard-credentials/regenerate"
                  onsubmit="return confirm('This immediately changes the dashboard password and restarts the account. Continue?')">
                <button type="submit" class="btn-secondary" style="color:var(--warn);border-color:var(--warn)">Regenerate password</button>
            </form>
            {creds_saved_html}
        </div>
    """))


@router.post("/accounts/{name}/keys")
async def update_account_keys(
    request: Request, name: str,
    sampling_key: str = Form(default=""), discovery_key: str = Form(default=""),
):
    if not _require_login(request):
        return RedirectResponse(f"{PREFIX}/login", status_code=303)
    try:
        await update_solana_tracker_keys(name, sampling_key, discovery_key)
    except ProvisioningError as exc:
        return RedirectResponse(f"{PREFIX}/accounts/{name}/settings?error={exc}", status_code=303)
    return RedirectResponse(f"{PREFIX}/accounts/{name}/settings?saved=keys", status_code=303)


@router.post("/accounts/{name}/trading-params")
async def update_account_trading_params(request: Request, name: str):
    if not _require_login(request):
        return RedirectResponse(f"{PREFIX}/login", status_code=303)
    form = await request.form()
    try:
        validated = {key: validate_trading_param(key, form.get(key, "")) for key in TRADING_PARAMS}
        await update_trading_params(name, validated)
    except ProvisioningError as exc:
        return RedirectResponse(f"{PREFIX}/accounts/{name}/settings?error={exc}&for=params", status_code=303)
    return RedirectResponse(f"{PREFIX}/accounts/{name}/settings?saved=params", status_code=303)


@router.post("/accounts/{name}/dashboard-credentials/regenerate")
async def regenerate_dashboard_credentials(request: Request, name: str):
    if not _require_login(request):
        return RedirectResponse(f"{PREFIX}/login", status_code=303)

    async with get_session() as session:
        acct = (await session.execute(
            select(ControlAccount).where(ControlAccount.name == name)
        )).scalar_one_or_none()
        if acct is None:
            return RedirectResponse(PREFIX + "/", status_code=303)
        user, password = generate_dashboard_credentials(name)
        try:
            await update_dashboard_credentials(name, user, password)
        except ProvisioningError as exc:
            return RedirectResponse(f"{PREFIX}/accounts/{name}/settings?error={exc}", status_code=303)
        acct.dashboard_auth_user = user
        acct.dashboard_auth_password = password

    return RedirectResponse(f"{PREFIX}/accounts/{name}/settings?saved=creds", status_code=303)


async def _fetch_account_notifications(acct: ControlAccount, limit: int) -> list[dict]:
    """One account's own /confluence/notifications, over HTTP on localhost
    (same host, same box — no need to round-trip through the public nginx
    domain) using its stored Basic Auth credentials. Never raises: one
    account being briefly unreachable must not blank the whole bell for
    every other account."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"http://127.0.0.1:{acct.port}/confluence/notifications",
                params={"limit": limit},
                auth=(acct.dashboard_auth_user, acct.dashboard_auth_password),
            )
            resp.raise_for_status()
            items = resp.json()
    except Exception as exc:
        log.warning("control_panel.notifications_fetch_failed", account=acct.name, error=str(exc))
        return []
    for item in items:
        item["account"] = acct.name
    return items


@router.get("/api/notifications")
async def api_notifications(request: Request, limit: int = Query(default=10, ge=1, le=50)):
    """
    Notification bell's data source (2026-10-01, "adopt AnchorLedger's
    notification/alert system") — aggregates every account's own
    ConfluenceNotification feed (already existed, api/app.py's
    /confluence/notifications) into one merged, sorted list. Never touches
    a trading account's database directly, same boundary as every other
    control-panel feature — this is an HTTP call to that account's own
    API, exactly like a browser hitting its dashboard would.
    """
    if not _require_login(request):
        return JSONResponse([], status_code=401)

    async with get_session() as session:
        accounts = (await session.execute(select(ControlAccount))).scalars().all()

    results = await asyncio.gather(*(_fetch_account_notifications(a, limit) for a in accounts))
    merged = [item for sub in results for item in sub]
    merged.sort(key=lambda n: n["created_at"], reverse=True)
    return JSONResponse(merged[:limit * 2])


app.include_router(router)


@app.get("/panel")
async def panel_root_redirect():
    """Bare /panel (no trailing slash) -> the real homepage route."""
    return RedirectResponse(PREFIX + "/", status_code=307)
