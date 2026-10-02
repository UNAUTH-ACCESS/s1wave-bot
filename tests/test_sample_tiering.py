import workers  # noqa: F401
from config.settings import settings
from workers.sampling_worker import sample_interval_s


def test_tiers():
    assert sample_interval_s(5, False, False) == 0
    assert sample_interval_s(29, False, False) == 0
    assert sample_interval_s(30, False, False) == 300
    assert sample_interval_s(119, False, False) == 300
    assert sample_interval_s(120, False, False) == 900
    assert sample_interval_s(5, True, False) == 600
    assert sample_interval_s(500, False, True) == 0


def test_disabled(monkeypatch):
    monkeypatch.setattr(settings, "SAMPLE_TIERING_ENABLED", False)
    assert sample_interval_s(500, True, False) == 0
