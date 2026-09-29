"""
control_panel/models.py
=========================
ORM models for the control panel's OWN database (s1wave_control,
2026-09-29) — deliberately a separate Base/schema from models.orm.Base
(the trading schema every S1Wave account uses). This process never
trades and has nothing to do with token discovery, momentum signals, or
positions — its whole job is "who can log in" and "which accounts
exist," so it gets its own small, independent set of tables rather than
bolting onto the trading models.

ControlAccount is a lightweight REGISTRY, not the account's own trading
data (which lives in that account's own database, e.g. s1wave_second) —
just enough to list accounts on the homepage and know where to reach
each one (its port) and which OS-level pieces belong to it (its .env
file, its systemd unit) for engine/provisioning.py to manage.
"""

from __future__ import annotations

import uuid as _uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _new_uuid() -> _uuid.UUID:
    return _uuid.uuid4()


class Base(DeclarativeBase):
    pass


class ControlUser(Base):
    """
    The person who can log into the control panel. Deliberately just one
    row in practice — see control_panel/app.py's /signup route, which
    refuses to create a second user. A real multi-user login system was
    explicitly ruled out (2026-09-29): this is a private admin panel for
    one owner's own trading accounts, not a public signup service.
    """

    __tablename__ = "control_users"

    id: Mapped[_uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    username: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(String(128), nullable=False)  # bcrypt, never plaintext
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def __repr__(self) -> str:
        return f"<ControlUser {self.username}>"


class ControlAccount(Base):
    """
    One row per S1Wave trading account this control panel knows about —
    written by engine/provisioning.py at creation time. `name` matches
    the account's .env.<name> suffix and systemd unit
    (s1wave-bot-<name>.service) exactly; the ORIGINAL, pre-multi-account
    deployment (.env, s1wave-bot.service, port 8000) is registered here
    too under the reserved name "base" so the homepage lists every real
    account, not just the ones created after this existed.
    """

    __tablename__ = "control_accounts"

    id: Mapped[_uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    name: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    port: Mapped[int] = mapped_column(Integer, nullable=False)
    wallet_pubkey: Mapped[str] = mapped_column(String(64), nullable=False)
    dashboard_auth_user: Mapped[str] = mapped_column(String(64), nullable=False)
    dashboard_auth_password: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def __repr__(self) -> str:
        return f"<ControlAccount {self.name} port={self.port}>"
