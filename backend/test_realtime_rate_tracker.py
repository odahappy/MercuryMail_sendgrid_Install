from app.services.realtime_rate_tracker import RealtimeRateTracker


def test_continuous_success_rate_tracks_real_events() -> None:
    tracker = RealtimeRateTracker(window_seconds=5, half_life_seconds=2, clock=lambda: 0.0)
    for second in range(5):
        tracker.record(success_count=10, processed_count=10, at=float(second))
    sample = tracker.sample(at=5.0)
    assert sample["success_count"] == 50
    assert sample["processed_count"] == 50
    assert sample["success_rate"] == 10.0


def test_rate_decays_each_second_without_new_successes() -> None:
    tracker = RealtimeRateTracker(window_seconds=5, half_life_seconds=2, clock=lambda: 0.0)
    tracker.record(success_count=50, processed_count=50, at=0.0)
    initial = tracker.sample(at=0.0)["success_rate"]
    after_six = tracker.sample(at=6.0)["success_rate"]
    after_eight = tracker.sample(at=8.0)["success_rate"]
    assert initial == 10.0
    assert 0 < after_six < initial
    assert 0 <= after_eight < after_six


def test_failure_does_not_increase_success_rate() -> None:
    tracker = RealtimeRateTracker(window_seconds=5, half_life_seconds=2, clock=lambda: 0.0)
    tracker.record(success_count=0, processed_count=20, at=0.0)
    sample = tracker.sample(at=1.0)
    assert sample["success_rate"] == 0.0
    assert sample["processed_rate"] > 0


def test_five_second_window_is_less_jumpy_than_one_second_window() -> None:
    short = RealtimeRateTracker(window_seconds=1, half_life_seconds=0.1, clock=lambda: 0.0)
    smooth = RealtimeRateTracker(window_seconds=5, half_life_seconds=2, clock=lambda: 0.0)
    for tracker in (short, smooth):
        tracker.record(success_count=20, processed_count=20, at=0.0)
        tracker.sample(at=0.0)
    short_idle = short.sample(at=1.1)["success_rate"]
    smooth_idle = smooth.sample(at=1.1)["success_rate"]
    assert smooth_idle > short_idle
