import pytest

import workers  # noqa: F401  (pre-existing engine.notify <-> workers import cycle)
from engine import notify as notify_mod
from engine.provisioning import ProvisioningError, _upsert_env, validate_notification_contacts


def test_validate_ok_and_blank():
    assert validate_notification_contacts(" a@b.co ", " -100123456 ") == ("a@b.co", "-100123456")
    assert validate_notification_contacts("", "") == ("", "")


@pytest.mark.parametrize("email,chat", [("nope", ""), ("a@b", ""), ("", "abc"), ("", "12")])
def test_validate_rejects(email, chat):
    with pytest.raises(ProvisioningError):
        validate_notification_contacts(email, chat)


def test_upsert_env_replaces_and_appends(tmp_path):
    p = tmp_path / ".env"
    p.write_text("A=1\nNOTIFY_EMAIL=old@x.co\n")
    _upsert_env(p, {"NOTIFY_EMAIL": "new@x.co", "TELEGRAM_CHAT_ID": "5"})
    assert p.read_text() == "A=1\nNOTIFY_EMAIL=new@x.co\nTELEGRAM_CHAT_ID=5\n"
    assert oct(p.stat().st_mode & 0o777) == "0o600"


def test_trade_chatter_is_not_emailed():
    assert "exit_filled" not in notify_mod.EMAIL_EVENTS
    assert "permanently_halted" in notify_mod.EMAIL_EVENTS
