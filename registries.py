from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Type
from pathlib import Path
import csv
import json
import bisect
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


class Registry:
    def __init__(self):
        self._items = {}

    def register(self, name: str, item):
        replacement = name in self._items
        self._items[name] = item
        return replacement

    def get(self, name: str, alt=None) -> Optional[Any]:
        return self._items.get(name, alt)

    @property
    def available(self) -> List[str]:
        return set(self._items.keys())
