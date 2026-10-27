from app.services.task_runner import _get_monitor_success_count


def test_monitor_success_count_uses_live_success_when_cached_count_is_stale() -> None:
    runtime = {
        "task_id": 7,
        "sent_count": 10,
        "initial_sent_count": 10,
        "success": 15,
        "live_success_count": 24,
    }
    assert _get_monitor_success_count(7, runtime) == 25


def test_monitor_success_count_is_task_isolated() -> None:
    runtime = {"task_id": 8, "sent_count": 100, "success": 100}
    assert _get_monitor_success_count(7, runtime) == 0
