import threading
from queue import Queue
from data_fetch_optimization.backoff import BackoffManager
from data_fetch_optimization.operation import OperationModel, RequestArg, Response
from data_fetch_optimization.log import MockLogger
import time
import typing as t
import logging


class FetchWriteCoordinator(t.Generic[RequestArg, Response]):
    """
    This class coordinates the fetching and writing of data.
    It contains all the logic of retries, which IO operations to prefer.
    """

    def __init__(
        self,
        max_threads: int,
        max_attempts_per_request: int,
        backoff_manager: BackoffManager,
        operations: OperationModel[RequestArg, Response],
        thread_sleep_seconds_if_no_work: float = 0.1,
        logger: logging.Logger = MockLogger(),  # type: ignore
    ):
        self.max_threads = max_threads
        self.max_attempts_per_request = max_attempts_per_request
        self.backoff_manager = backoff_manager
        self.api_fetch_queue: Queue[t.Callable[[], None]] = Queue()
        self.write_queue: Queue[t.Callable[[], None]] = Queue()  # type: ignore
        self.active_api_fetch_tasks = 0
        self.counter_lock = threading.Lock()
        self.operations = operations
        self.thread_sleep_seconds_if_no_work = thread_sleep_seconds_if_no_work
        self.logger = logger

    def _initialize_api_fetch_queue(self):
        for api_fetch_request_arg in self.operations.initial_operation():
            self.logger.debug(
                "Adding request arg: %s, to api fetch task queue", api_fetch_request_arg
            )
            self.api_fetch_queue.put(
                lambda arg=api_fetch_request_arg, attempt=1: self._api_fetch_task(
                    arg, attempt
                )
            )

    def _api_fetch_task(self, request_argument: RequestArg, request_attempt: int):
        response = self.operations.fetch_from_api(request_argument)
        if not self.operations.response_succeeded(response):
            self.backoff_manager.increase_backoff()
            if request_attempt < self.max_attempts_per_request:
                self.api_fetch_queue.put(
                    lambda arg=request_argument, attempt=request_attempt + 1: self._api_fetch_task(  # noqa
                        arg,
                        attempt,
                    )
                )
            else:
                self.logger.error(
                    "Request failed for api fetch task with arg: %s and response: %s",
                    request_argument,
                    response,
                )
                self.operations.upon_definitive_request_failure(
                    request_argument, response
                )
        else:
            self.logger.info(
                "Request succeeded for arg: %s on attempt nr: %s",
                request_argument,
                request_attempt,
            )
            self.backoff_manager.reset_backoff()
            self.write_queue.put(
                lambda arg=request_argument, resp=response: self.operations.write_fetched_data(  # noqa
                    arg, resp
                )
            )

    def _worker(self):
        while (
            not self.api_fetch_queue.empty()
            or self.active_api_fetch_tasks > 0
            or not self.write_queue.empty()
        ):
            if not self.api_fetch_queue.empty() and (
                self.write_queue.empty() or self.backoff_manager.should_back_off()
            ):
                # Prefer API fetch task unless the API is in backoff
                # or the write queue is larger
                api_fetch_task = self.api_fetch_queue.get()
                with self.counter_lock:
                    self.active_api_fetch_tasks += 1
                api_fetch_task()
                with self.counter_lock:
                    self.active_api_fetch_tasks -= 1
            elif not self.write_queue.empty():
                # If there are tasks in the write queue, perform write to disk
                write_task = self.write_queue.get()
                write_task()
            else:
                # If both queues are empty, check again after brief pause
                time.sleep(self.thread_sleep_seconds_if_no_work)

    def process(self):
        self._initialize_api_fetch_queue()
        threads: t.List[threading.Thread] = []
        self.logger.info("Going to initialize %s threads", self.max_threads)
        for _ in range(self.max_threads):
            thread = threading.Thread(target=self._worker)
            thread.start()
            threads.append(thread)

        # Join all threads
        for thread in threads:
            thread.join()
