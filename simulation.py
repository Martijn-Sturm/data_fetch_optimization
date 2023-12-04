import logging
from data_fetch_optimization.demo import DemoOperations
from data_fetch_optimization.processing import FetchWriteCoordinator
from data_fetch_optimization.backoff import BackoffManager

# Define the format of the log message
log_format = "%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s"
# log_format = "%(asctime)s.%(msecs)03d - %(message)s - %(arg)s"
date_format = "%Y-%m-%d %H:%M:%S"

# Create a logger
logger = logging.getLogger("demo_analysis")
logger.setLevel(logging.DEBUG)

# Create a file handler that logs to a file
log_file = "my_application.log"
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the file handler
formatter = logging.Formatter(log_format, datefmt=date_format, style="%")
file_handler.setFormatter(formatter)


# # Add a filter to inject extra properties
# class ContextFilter(logging.Filter):
#     def filter(self, record):
#         record.arg = getattr(record, "arg", "N/A")
#         return True


# logger.addFilter(ContextFilter())

# Add the file handler to the logger
logger.addHandler(file_handler)

operations = DemoOperations(
    n_fetches=10,
    api_fetch_delay_sequence=[2, 2, 3, 3, 3, 7, 7, 10, 10, 20],
    response_successful=[True, True, False, False, False],
    write_delay_sequence=[10, 10, 15, 15, 20, 20, 20, 30, 40],
    logger=logger,
)

logger_backoff = logger.getChild("backoff_manager")
backoff = BackoffManager(
    initial_delay_seconds=1,
    max_delay_seconds=1 * 60,
    backoff_factor=2,
    max_seconds_in_subsequent_backoff=5 * 60,
    logger=logger_backoff,
)

processor = FetchWriteCoordinator(
    max_threads=5,
    max_attempts_per_request=3,
    backoff_manager=backoff,
    operations=operations,
    logger=logger,
    thread_sleep_seconds_if_no_work=1,
)

processor.process()
