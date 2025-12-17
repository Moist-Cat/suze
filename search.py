from abc import ABC, abstractmethod
import bisect
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable, Union
from pathlib import Path
import csv
import json
import operator
import re

from bs4 import BeautifulSoup

from suze.log import get_logger


logger = get_logger("search")


def matches_query(item: Dict, query: str, key: str, dependencies: Dict) -> bool:
    search_columns = [key] if key else item.keys()
    query = re.compile(query)

    for column in search_columns:
        if re.search(query, item[column]):
            return True
    return False


# linear
def linear_search(
    data: Any,
    dependencies,
    query: Any = None,
    key: str = None,
    match_function=None,
    remove_duplicates=False,
):
    """
    Perform a linear search on `data`. Add a result
    if `item` in `key` matches the query.
    `remove_duplicates` breaks early when finding a match.
    """
    match_function = match_function or matches_query
    if not query:
        return data

    if isinstance(query, Callable):
        query = query(key, dependencies)
    if not isinstance(query, list):
        query = [query]

    results = []
    for req in query:
        for item in data:
            if match_function(item, req, key, dependencies):
                results.append(item)
                if remove_duplicates:
                    break

    logger.info("Matched %d results", len(results))
    return results


def binary_search(data, dependencies, query: Any = None, key: str = None):
    """
    Assumes the items are sorted
    """
    if not query or not key:
        return data

    if isinstance(query, Callable):
        query = query(key, dependencies)
    if not isinstance(query, list):
        query = [query]

    # Binary search for efficient lookup
    results = []
    for req in query:
        idx = bisect.bisect_left(data, req, key=operator.itemgetter(key))
        idx_end = bisect.bisect_right(data, req, key=operator.itemgetter(key))
        # guard for 0 and end of array cases
        if idx < len(data) and data[idx][key] == req:
            if idx + 1 != idx_end:
                results.extend(data[idx:idx_end])
            else:
                results.append(data[idx])
    logger.info("Matched %d results", len(results))
    return results


# html
def html_search(html_content, dependencies, *args, get_attrs=None, **kwargs):
    """
    Search strategy for HTML ASTs using BeautifulSoup4.
    """
    if not args and not kwargs and not get_attrs:
        return html_content

    get_text = "full_text"
    if not get_attrs:
        get_attrs = [get_text]
    fetch_all = False
    if "*" in get_attrs:
        fetch_all = True

    if not isinstance(html_content, list):
        html_content = [html_content]
    results = []
    for page in html_content:
        soup = BeautifulSoup(page, "html.parser")
        res = soup.find_all(*args, **kwargs)
        for item in res:
            itemey = {}
            if fetch_all:
                itemey = item.attrs.copy()
                itemey[get_text] = item.text
            else:
                for attr in get_attrs:
                    if attr == get_text:
                        itemey[get_text] = item.text
                        continue
                    itemey[attr] = item[attr]
            results.append(itemey)
    return results
