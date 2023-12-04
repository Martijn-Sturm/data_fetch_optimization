from data_fetch_optimization.backoff import BackoffManager
import time


def test_backoff_manager_exceeded_max_seconds_in_subsequent_backoff():
    backoff_manager = BackoffManager(
        initial_delay_seconds=1,
        max_delay_seconds=5 * 60,
        backoff_factor=2,
        max_seconds_in_subsequent_backoff=10,
    )
    backoff_manager.increase_backoff()
    assert not backoff_manager.has_exceeded_max_time_in_subsequent_backoff()
    time.sleep(12)
    assert backoff_manager.has_exceeded_max_time_in_subsequent_backoff()
