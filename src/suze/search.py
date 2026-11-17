import bisect
from typing import Any, Dict, Callable
import operator
import re

from bs4 import BeautifulSoup

from suze.log import get_logger


logger = get_logger("search")


def matches_query(
        item: Any,
        query: str,
        key: str,
        dependencies: Dict | None = None,
        accessor=None
    ) -> bool:
    del dependencies  # default doesn't use it
    accessor = accessor or item.__getitem__
    if key:
        search_columns = [key]
    else:
        if hasattr(item, "keys"):
            search_columns = item.keys()
        else:
            # the whole item
            search_columns = [item]

    query = re.compile(query)

    for column in search_columns:
        if re.search(query, accessor(column)):
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


def text_search(data: str, dependencies, query=None, key=None) -> list:
    """Substring search over a string or list of strings.

    Complements `linear_search` (which requires list[dict]). Used by
    FileDataSource for .txt and .log files.
    """
    del dependencies, key
    if not query:
        return data
    if isinstance(data, str):
        lines = data.splitlines()
    else:
        lines = list(data)
    return [line for line in lines if isinstance(line, str) and query in line]

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
def html_search(data: str | list[str], dependencies, *args, get_attrs=None, search_attrs=None, query=None, fill=True, **kwargs) -> list:
    """
    Search strategy for HTML ASTs using BeautifulSoup4.
    `fill`: Wheter to fill with empty records to return a result of the same size.
        It helps to keep a record of empty/invalid values and is on by default.
    """
    del dependencies  # contextual search is unnecessary in web pages

    if not args and not kwargs and not get_attrs:
        return data

    get_text = "full_text"
    if not get_attrs:
        get_attrs = [get_text]
    if not search_attrs:
        search_attrs = []

    if query is not None and not (len(search_attrs) == 1):
        raise ValueError(
            "query is only supported for requests that only fetch the text present in the page."
            "One query that covers multiple (or no) attributes is ambiguous."
        )

    fetch_all = False
    if "*" in get_attrs:
        fetch_all = True

    if not isinstance(data, list):
        data = [data]
    results = []
    for page in data:
        soup = BeautifulSoup(page, "html.parser")
        res = soup.find_all(*args, **kwargs)
        if not res and fill:
            # add empty result
            results.append({})
        for item in res:
            itemey = {}
            if fetch_all:
                itemey = item.attrs.copy()
                itemey[get_text] = item.text
            else:
                # filter
                if search_attrs == [get_text]:
                    accessor = lambda _: i.text
                else:
                    # default
                    accessor = None
                if query and not matches_query(item, query, search_attrs, accessor=accessor):
                    continue
                        
                for attr in get_attrs:
                    if attr == get_text:
                        val = item.text
                    else:
                        val = item[attr]

                    itemey[attr] = val
            results.append(itemey)
    return results
