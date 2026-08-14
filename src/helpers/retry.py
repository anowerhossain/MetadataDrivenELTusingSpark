"""
Reusable Operational Retry Handler Helper for Metadata-Driven PySpark ETL Framework on CDP.
Retries transient operational failures while failing fast on configuration and validation errors.
Supports both [execution] and [retry] TOML configuration tables.
"""

import time
from typing import Callable, TypeVar, Tuple, Type, Union
from src.helpers.logger import setup_logger
from src.core.config import RetrySection, ExecutionSection, ConfigError
from src.core.quality import DataQualityError

logger = setup_logger("RetryHandler")

T = TypeVar("T")

# Exception tuple that should NEVER be retried (fail fast)
NON_RETRYABLE_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    ConfigError,
    DataQualityError,
    FileNotFoundError,
    NotImplementedError,
    KeyError,
    ValueError,
    TypeError,
)


class RetryHandler:
    """Executes callables with configurable delay retries for transient operational errors."""

    def __init__(self, retry_config: Union[RetrySection, ExecutionSection]):
        if isinstance(retry_config, ExecutionSection):
            self.max_attempts = retry_config.retries
            self.delay_seconds = retry_config.retry_delay_seconds
            self.backoff_multiplier = getattr(retry_config, "backoff_multiplier", 2.0)
            self.exponential_backoff = getattr(retry_config, "exponential_backoff", True)
            self.enabled = True
            self.config = retry_config
        elif isinstance(retry_config, RetrySection):
            self.max_attempts = retry_config.max_attempts
            if hasattr(retry_config, "delay_seconds") and retry_config.delay_seconds != 30.0:
                self.delay_seconds = retry_config.delay_seconds
            else:
                self.delay_seconds = getattr(retry_config, "retry_delay_seconds", getattr(retry_config, "delay_seconds", 30.0))
            self.backoff_multiplier = getattr(retry_config, "backoff_multiplier", 2.0)
            self.exponential_backoff = getattr(retry_config, "exponential_backoff", True)
            self.enabled = getattr(retry_config, "enabled", True)
            self.config = retry_config
        else:
            raise ConfigError(f"Expected RetrySection or ExecutionSection configuration, got {type(retry_config).__name__}.")

    def calculate_delay(self, attempt_index: int) -> float:
        """Calculates wait delay for attempt_index (1-indexed)."""
        if not self.exponential_backoff or attempt_index <= 1:
            return float(self.delay_seconds)

        return float(self.delay_seconds * (self.backoff_multiplier ** (attempt_index - 1)))

    def execute(self, func: Callable[[], T], task_name: str = "Task") -> T:
        """Executes func with retries according to ExecutionSection/RetrySection configuration."""
        if not self.enabled:
            return func()

        total_attempts: int = self.max_attempts

        for attempt in range(1, total_attempts + 1):
            try:
                return func()
            except NON_RETRYABLE_EXCEPTIONS as err:
                logger.warning(
                    f"[{task_name}] Encountered non-retryable exception '{type(err).__name__}': {err}. "
                    f"Failing fast without retry."
                )
                raise err
            except Exception as err:
                if attempt == total_attempts:
                    logger.error(
                        f"[{task_name}] Attempt {attempt}/{total_attempts} failed with transient error: {err}. "
                        f"Final attempt reached. Aborting."
                    )
                    raise err

                current_delay = self.calculate_delay(attempt)
                logger.warning(
                    f"[{task_name}] Attempt {attempt}/{total_attempts} failed with transient error: {err}. "
                    f"Retrying in {current_delay:.1f} seconds..."
                )
                if current_delay > 0:
                    time.sleep(current_delay)

        return func()
