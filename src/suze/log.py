"""
Logging
"""

import asyncio
from pathlib import Path
import time
import logging
import logging.handlers
import functools
import sys
from datetime import datetime

from suze.config import BASE_DIR


# Create module-specific loggers
def get_logger(name: str) -> logging.Logger:
    """Get a named logger for specific components"""
    return logging.getLogger(f"user_info.{name}")


def logged(cls):
    "Class decorator for logging purposes"

    cls.logger = logging.getLogger("user_info." + cls.__qualname__)
    cls.logger_err = logging.getLogger("audit." + cls.__qualname__)

    return cls
