"""
Framework Helper Utilities Package for Cloudera CDP Platform.
Contains helper modules for SparkSession creation, structured logging, resource tuning,
retries, failure marker tracking, and success marker tracking.
"""

from src.helpers.logger import setup_logger, ETLLogger, JobMetrics
from src.helpers.spark import SparkSessionFactory, get_cdp_spark_session
from src.helpers.tuner import SparkResourceTuner
from src.helpers.retry import RetryHandler
from src.helpers.failure import FailureHandler
from src.helpers.success import SuccessHandler
from src.helpers.email_notification import EmailNotification
from src.helpers.email_template import EmailTemplateManager

__all__ = [
    "setup_logger",
    "ETLLogger",
    "JobMetrics",
    "SparkSessionFactory",
    "get_cdp_spark_session",
    "SparkResourceTuner",
    "RetryHandler",
    "FailureHandler",
    "SuccessHandler",
    "EmailNotification",
    "EmailTemplateManager",
]
