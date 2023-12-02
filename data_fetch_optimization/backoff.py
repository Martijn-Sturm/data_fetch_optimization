import time
import threading


class BackoffManager:
    def __init__(
        self,
        initial_backoff: int = 1,
        max_backoff: int = 5 * 60,
        backoff_factor: float = 2,
    ):
        self.lock = threading.Lock()
        self.backoff_time = 0
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_factor = backoff_factor
        self.current_backoff = initial_backoff

    def should_back_off(self):
        with self.lock:
            return time.time() < self.backoff_time

    def increase_backoff(self):
        with self.lock:
            self.current_backoff = min(
                self.current_backoff * self.backoff_factor, self.max_backoff
            )
            self.backoff_time = time.time() + self.current_backoff

    def reset_backoff(self):
        with self.lock:
            self.current_backoff = self.initial_backoff
            self.backoff_time = 0

    def get_backoff_time(self):
        with self.lock:
            return self.backoff_time - time.time() if self.should_back_off() else 0
