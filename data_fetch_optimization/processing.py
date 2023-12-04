import threading
from queue import Queue
from data_fetch_optimization.backoff import BackoffManager
from data_fetch_optimization.operation import OperationModel, RequestArg, Response
from data_fetch_optimization.log import MockLogger
import time
import typing as t
import logging
import random


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

    def _handle_failed_request(
        self, request_argument: RequestArg, response: Response, request_attempt: int
    ):
        self.logger.warning("A request attempt failed for arg: %s", request_argument)
        self.backoff_manager.increase_backoff()
        if request_attempt < self.max_attempts_per_request:
            if not self.backoff_manager.has_exceeded_max_time_in_subsequent_backoff():
                self.logger.debug(
                    "adding retry api fetch task to queue with "
                    "request arg: %s and with attempt nr: %s",
                    request_argument,
                    request_attempt,
                )
                self.api_fetch_queue.put(
                    lambda arg=request_argument, attempt=request_attempt + 1: self._api_fetch_task(  # noqa
                        arg,
                        attempt,
                    )
                )
                return
            else:
                self.logger.debug(
                    "Not adding retry api fetch task to queue with "
                    "request arg: %s and with attempt nr: %s, "
                    "as the API is in backoff for too long",
                    request_argument,
                    request_attempt,
                )

        self.logger.error(
            "Request failed for api fetch task with arg: %s and response: %s",
            request_argument,
            response,
        )
        self.operations.upon_definitive_request_failure(request_argument, response)

    def abort_upon_exceeding_max_time_in_subsequent_backoff(
        self, request_argument: RequestArg, request_attempt: int
    ) -> bool:
        if self.backoff_manager.has_exceeded_max_time_in_subsequent_backoff():
            self.logger.debug("aborting api fetch task")
            self.operations.upon_request_abortion(
                request_argument,
                "API longer in backoff than set max seconds: "
                f"{self.backoff_manager.max_seconds_in_subsequent_backoff}, "
                f"aborted at attempt nr: {request_attempt}",
            )
            return True
        else:
            self.logger.debug("API not in backoff for too long to abort")
            return False

    def _api_fetch_task(  # noqa
        self, request_argument: RequestArg, request_attempt: int
    ):
        if self.abort_upon_exceeding_max_time_in_subsequent_backoff(
            request_argument, request_attempt
        ):
            return
        wait = self.backoff_manager.get_wait_seconds()
        while wait > 0 or not self.backoff_manager.acquire_api_call_permission():
            if wait == 0:
                wait = self.backoff_manager.get_wait_seconds()
            self.logger.debug(
                "Waiting %s seconds",
                wait,
            )
            time.sleep(wait + random.random() / 10)
            wait = self.backoff_manager.get_wait_seconds()
            if self.abort_upon_exceeding_max_time_in_subsequent_backoff(
                request_argument, request_attempt
            ):
                return

        self.logger.debug(
            "Making API call for arg: %s and attempt nr: %s",
            request_argument,
            request_attempt,
        )
        response = self.operations.fetch_from_api(request_argument)
        if not self.operations.response_succeeded(response):
            self._handle_failed_request(request_argument, response, request_attempt)
        else:
            self.logger.info(
                "Request succeeded for arg: %s on attempt nr: %s",
                request_argument,
                request_attempt,
            )
            self.backoff_manager.reset_backoff()
            self.logger.debug("Adding write task to write queue")
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
            if not self.api_fetch_queue.empty() and self.write_queue.empty():
                # Prefer API fetch task unless the API is in backoff
                # or the write queue is larger
                self.logger.debug("thread is picking up api fetch task")
                api_fetch_task = self.api_fetch_queue.get()
                with self.counter_lock:
                    self.active_api_fetch_tasks += 1
                api_fetch_task()
                with self.counter_lock:
                    self.active_api_fetch_tasks -= 1
            elif not self.write_queue.empty():
                # If there are tasks in the write queue, perform write to disk
                self.logger.debug("thread is picking up write task")
                write_task = self.write_queue.get()
                write_task()
            else:
                # If both queues are empty, check again after brief pause
                self.logger.debug("thread going to sleep")
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
