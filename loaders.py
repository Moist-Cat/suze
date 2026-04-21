"""Data loading utilities for various file formats and network sources.

This module provides asynchronous data loaders for text files, structured data
formats (JSON, CSV, TSV), log files, HTTP resources, YouTube content, and
data transformation operations.
"""

import json
import csv
from pathlib import Path
import asyncio
import aiohttp
from typing import Callable

try:
    from yt_dlp import YoutubeDL as yt
except ImportError:
    yt = None

from suze.log import get_logger
from suze.retry import with_retry, RetryConfig


logger = get_logger("loader")


# I/O
# file system
def _sync_read(file_path: Path) -> str:
    """Synchronously reads a text file from the filesystem.

    Args:
        file_path: Path to the file to read.

    Returns:
        The file contents as a string.

    Raises:
        IOError: If the file cannot be read.
        UnicodeDecodeError: If the file contains invalid UTF-8 characters.

    Example:
        >>> content = _sync_read(Path("data.txt"))
    """
    logger.debug(f"Reading file synchronously: {file_path}")
    try:
        with open(file_path, encoding="utf8") as fd:
            content = fd.read()
            logger.debug(
                f"Successfully read {len(content)} characters from {file_path}"
            )
            return content
    except Exception as e:
        logger.error(f"Failed to read file {file_path}: {str(e)}")
        raise


async def text_loader(file_path: Path) -> str:
    """Asynchronously loads a text file.

    Args:
        file_path: Path to the text file to load.

    Returns:
        The file contents as a string.

    Raises:
        IOError: If the file cannot be read.

    Example:
        >>> content = await text_loader(Path("data.txt"))
    """
    logger.info(f"Loading text file: {file_path}")
    return await asyncio.to_thread(_sync_read, file_path)


async def json_loader(file_path: Path):
    """Asynchronously loads and parses a JSON file.

    Args:
        file_path: Path to the JSON file to load.

    Returns:
        Parsed JSON data (typically dict or list).

    Raises:
        IOError: If the file cannot be read.
        json.JSONDecodeError: If the file contains invalid JSON.

    Example:
        >>> data = await json_loader(Path("config.json"))
    """
    logger.info(f"Loading JSON file: {file_path}")
    content = await text_loader(file_path)
    try:
        data = json.loads(content)
        logger.debug(f"Successfully parsed JSON from {file_path}")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from {file_path}: {str(e)}")
        raise


async def tsv_loader(file_path: Path):
    """Asynchronously loads and parses a TSV (Tab-Separated Values) file.

    Args:
        file_path: Path to the TSV file to load.

    Returns:
        List of dictionaries where each dictionary represents a row,
        with column names as keys.

    Raises:
        IOError: If the file cannot be read.
        csv.Error: If the TSV format is invalid.

    Example:
        >>> rows = await tsv_loader(Path("data.tsv"))
        >>> # rows[0] = {'col1': 'value1', 'col2': 'value2', ...}
    """
    logger.info(f"Loading TSV file: {file_path}")
    content = await text_loader(file_path)
    try:
        data = list(csv.DictReader(content.splitlines(), delimiter="\t", quotechar='"'))
        logger.debug(f"Successfully parsed {len(data)} rows from TSV file {file_path}")
        return data
    except Exception as e:
        logger.error(f"Failed to parse TSV from {file_path}: {str(e)}")
        raise


async def csv_loader(file_path: Path):
    """Asynchronously loads and parses a CSV (Comma-Separated Values) file.

    Args:
        file_path: Path to the CSV file to load.

    Returns:
        List of dictionaries where each dictionary represents a row,
        with column names as keys. Uses comma as delimiter.

    Raises:
        IOError: If the file cannot be read.
        csv.Error: If the CSV format is invalid.

    Example:
        >>> rows = await csv_loader(Path("data.csv"))
        >>> # rows[0] = {'col1': 'value1', 'col2': 'value2', ...}
    """
    logger.info(f"Loading CSV file: {file_path}")
    content = await text_loader(file_path)
    try:
        data = list(csv.DictReader(content.splitlines(), quotechar='"'))
        logger.debug(f"Successfully parsed {len(data)} rows from CSV file {file_path}")
        return data
    except Exception as e:
        logger.error(f"Failed to parse CSV from {file_path}: {str(e)}")
        raise


async def log_loader(file_path: Path):
    """Asynchronously loads a log file, splitting it into lines.

    Args:
        file_path: Path to the log file to load.

    Returns:
        List of strings, where each string is a line from the log file.

    Example:
        >>> lines = await log_loader(Path("app.log"))
        >>> # lines = ['2023-01-01 INFO: Starting app', ...]
    """
    logger.info(f"Loading log file: {file_path}")
    content = await text_loader(file_path)
    lines = content.splitlines()
    logger.debug(f"Loaded {len(lines)} lines from log file {file_path}")
    return lines


# net
@with_retry(RetryConfig(max_attempts=10))
async def http_loader(session, url, method, request_data, headers, raw=False):
    """Asynchronously loads data from an HTTP endpoint.

    Args:
        session: aiohttp.ClientSession instance for making requests.
        url: The URL to request.
        method: HTTP method (e.g., 'GET', 'POST').
        request_data: Dictionary of request parameters (passed to aiohttp).
        headers: Dictionary of HTTP headers.
        raw: If True, returns the raw aiohttp response object.
            If False (default), processes the response based on content type.

    Returns:
        If raw=True: aiohttp.ClientResponse object.
        If raw=False and content-type is JSON: parsed JSON data.
        If raw=False and content-type is not JSON: response text as string.

    Raises:
        aiohttp.ClientResponseError: For HTTP error status codes (≥400).
        aiohttp.ClientError: For network or connection errors.

    Example:
        >>> session = aiohttp.ClientSession()
        >>> data = await http_loader(
        ...     session=session,
        ...     url="https://api.example.com/data",
        ...     method="GET",
        ...     request_data={},
        ...     headers={"Authorization": "Bearer token"}
        ... )
    """
    logger.info(f"Making HTTP {method} request to: {url}")
    logger.debug(f"Request headers: {headers}")
    logger.debug(f"Request data: {request_data}")

    try:
        async with session.request(
            method=method, url=url, headers=headers, **request_data
        ) as response:
            if raw:
                return response
            # Get response content
            content = await response.read()
            content_length = len(content)

            logger.debug(
                f"Response status: {response.status}, Content length: {content_length} bytes"
            )

            if response.status >= 400:
                logger.warning(f"HTTP error {response.status} for URL: {url}")
                # Raise HTTP error for error status codes
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"HTTP {response.status}",
                    headers=response.headers,
                )

            # Return appropriate format based on content type
            content_type = response.headers.get("content-type", "").lower()
            logger.debug(f"Response content-type: {content_type}")

            if "application/json" in content_type:
                logger.debug("Parsing response as JSON")
                return await response.json()
            else:
                logger.debug("Returning response as text")
                return content.decode("utf-8")

    except Exception as e:
        logger.error(f"HTTP request failed for {url}: {str(e)}")
        raise


async def yt_loader(data, search_limit=5):
    """Asynchronously searches YouTube for trailer videos based on movie data.

    Args:
        data: Dictionary containing movie metadata. Must include:
            - primaryTitle: The title of the movie/TV show.
            - startYear: The release year.
            - tconst: (Optional) IMDb identifier, will be preserved in results.
        search_limit: Maximum number of YouTube results to return (default: 5).

    Returns:
        List of dictionaries containing YouTube video metadata. Each dictionary
        includes a subset of YouTube video properties and the original movie
        metadata (tconst, primaryTitle, startYear).

        Returns empty list if required fields are missing from input data.

    Example:
        >>> movie_data = {
        ...     'primaryTitle': 'Inception',
        ...     'startYear': '2010',
        ...     'tconst': 'tt1375666'
        ... }
        >>> videos = await yt_loader(movie_data, search_limit=3)
        >>> # videos[0] = {
        >>> #     'id': 'video_id',
        >>> #     'title': 'Inception Official Trailer',
        >>> #     'tconst': 'tt1375666',
        >>> #     'primaryTitle': 'Inception',
        >>> #     'startYear': '2010',
        >>> #     ... other YouTube metadata
        >>> # }
    """
    if yt is None:
        raise ImportError("Please install yt-dlp")
    if not ("primaryTitle" in data and "startYear" in data):
        return []

    search_query = f"{data['primaryTitle']} {data['startYear']} Official Trailer"

    def sync_yt_loader(search_query=search_query):
        """Synchronous YouTube search using yt-dlp."""
        result = []
        KEYS = {
            "id",
            "title",
            "webpage_url",
            "duration",
            "duration_string",
            "view_count",
            "like_count",
            "description",
            "channel",
            "uploader",
            "channel_id",
            "uploader_id",
            "channel_url",
            "channel_follower_count",
            "upload_date",
            "categories",
            "tags",
            "comment_count",
            "average_rating",
            "channel_is_verified",
            "channel_follower_count",
        }
        options = {
            "ignore_no_formats_error": True,
            "ignoreerrors": "only_download",
        }
        with yt(options) as ydl:
            results = ydl.extract_info(
                f"ytsearch{search_limit}:{search_query}", download=False
            )
            videos = results.get("entries", [])[:search_limit]
            for idx in range(len(videos)):
                video = videos[idx]
                for key in video.copy().keys():
                    if key not in KEYS:
                        del video[key]
                # preserve metadata from previous step
                video["tconst"] = data["tconst"]
                video["primaryTitle"] = data["primaryTitle"]
                video["startYear"] = data["startYear"]
            result.extend(videos)
        return result

    return await asyncio.to_thread(sync_yt_loader)


# in memory
async def merge_loader(
    data_dict,
    target=None,
    merge_strategy="aggregate",
    selector=None,
    auto_index=False,
    strict=True,
):
    """Merges data from multiple sources based on a target field.

    This function performs an in-memory join operation similar to SQL JOIN.
    It can also function as a filter when no target is specified.

    Args:
        data_dict: Dictionary where keys are source identifiers and values
            are data (lists or single items) from each source.
        target: Field name to join on. If None, no merging is performed and
            data is returned as a list of records.
        merge_strategy: How to handle field conflicts when merging:
            - "aggregate": Collect conflicting values into a list (default).
            - "overwrite": Use the latest value.
            - "ignore": Keep the first value.
            - Callable: A custom function that receives (old_value, new_value,
              merged_record, new_record) and returns the merged value.
        selector: Set of source identifiers to include in the merge.
            If None, includes all sources.
        auto_index: If True, automatically generates target field values
            based on record position when target field is missing.
        strict: If True, only merges records that exist in the master table
            (first source in data_dict). If False, creates new entries for
            records from any source.

    Returns:
        List of merged dictionaries. When target is None, returns a flattened
        list of all records.

    Raises:
        ValueError: If merge_strategy is unknown or if target field contains
            list values.

    Example:
        >>> data = {
        ...     'source1': [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}],
        ...     'source2': [{'id': 1, 'age': 30}, {'id': 2, 'age': 25}]
        ... }
        >>> merged = await merge_loader(data, target='id')
        >>> # merged = [
        >>> #     {'id': 1, 'name': 'Alice', 'age': 30},
        >>> #     {'id': 2, 'name': 'Bob', 'age': 25}
        >>> # ]
    """
    if not data_dict:
        return data_dict
    logger.info(
        f"Starting merge loader with target field: '{target}', strategy: '{merge_strategy}'"
    )
    logger.debug(f"Input data has {len(data_dict)} sources")

    table = []
    lookup_table = {}
    selector = selector or set(data_dict.keys())
    # order is kept
    master_table = list(data_dict)[0]

    for dep_id, data in data_dict.items():
        # sanity check
        if auto_index:
            baseline = len(data_dict[master_table])
            msg = (
                "Auto-indexing is enabled but"
                f"the dependencies ({data_dict.keys()}) don't have "
                f"the same number of items ({len(data)}, {baseline})"
            )
            check = len(data) == baseline
            assert check, msg
        logger.debug(f"Processing data from source: {dep_id}")
        if dep_id not in selector:
            logger.debug(
                "Skipping dependency in merge ('%s' is not in selector)", dep_id
            )
            continue

        if not isinstance(data, list):
            data = [data]
            logger.warning("Converted non-list data to list with 1 element")

        logger.debug(f"Processing {len(data)} records from source {dep_id}")

        for record_idx, record in enumerate(data):
            if not target:
                table.append(record)
                continue

            if not isinstance(record, dict):
                logger.debug(f"Skipping non-dict record: {record}")
                continue

            if target not in record:
                if auto_index:
                    record[target] = table[record_idx][target]
                    logger.debug(
                        "Adding automatic index (%s) for for %s", target, record
                    )
                else:
                    logger.error(
                        f"Target field '{target}' not found in {record} from source {dep_id}"
                    )
                    # we don't raise an error because it could
                    # be a missing field
                    # that said, we can not merge it here
                    continue

            on_value = record[target]
            if on_value is None and not auto_index:
                logger.debug(
                    f"Skipping record with no value for target field '{target}'"
                )
                continue

            merged = {}
            if isinstance(on_value, list):
                raise ValueError(f"Invalid key value {target}: {on_value} in {record}")
            if on_value not in lookup_table:
                if (dep_id != master_table) and strict:
                    # Can not join records
                    logger.debug(f"Can not join records: {on_value}")
                    continue
                idx = len(table)
                lookup_table[on_value] = idx
                table.append(merged)
                logger.debug(f"Created new merged entry for target value: {on_value}")
            else:
                idx = lookup_table[on_value]
                merged = table[idx]
                logger.debug(
                    f"Using existing merged entry for target value: {on_value}"
                )

            for field, value in record.items():
                if field == target:
                    merged[field] = value
                    continue

                if field in merged:
                    logger.debug(
                        f"Field conflict for '{field}' in target '{on_value}' - applying {merge_strategy} strategy"
                    )
                    # Apply merge strategy
                    if merge_strategy == "aggregate":
                        if isinstance(merged[field], list):
                            merged[field].append(value)
                            logger.debug(
                                f"Aggregated value to existing list for field '{field}'"
                            )
                        else:
                            merged[field] = [merged[field], value]
                            logger.debug(
                                f"Created new list for field '{field}' with aggregated values"
                            )
                    elif merge_strategy == "overwrite":
                        merged[field] = value
                        logger.debug(f"Overwrote field '{field}' with new value")
                    elif merge_strategy == "ignore":
                        # Keep existing value
                        logger.debug(
                            f"Ignored new value for field '{field}' (keeping existing)"
                        )
                        pass
                    elif merge_strategy == "raise":
                        raise ValueError(
                            f"Duplicate field '{field}' detected in {dep_id}"
                        )
                    elif isinstance(merge_strategy, Callable):
                        logger.debug(
                            f"Using callable {merge_strategy} to decide what to do"
                        )
                        merged[field] = merge_strategy(
                            merged[field], value, field, merged, record
                        )

                    else:
                        logger.error(f"Unknown merge strategy: {merge_strategy}")
                        raise ValueError(f"Unknown merge strategy {merge_strategy}")
                else:
                    merged[field] = value
                    logger.debug(f"Set field '{field}' with new value")

    logger.info(f"Merge completed. Final table has {len(table)} entries")
    return table


async def split_loader(requests, return_exceptions=False):
    """Executes multiple data loading requests concurrently.

    This function is useful for parallelizing independent data loading operations.

    Args:
        requests: List of request objects that have a `fetch()` method.
            Each request should return awaitable data.

    Returns:
        List of results from all requests, in the same order as the input.

    Example:
        >>> requests = [HttpRequest(url1), HttpRequest(url2), FileRequest(path)]
        >>> results = await split_loader(requests)
        >>> # results = [data_from_url1, data_from_url2, file_contents]
    """
    logger.info(f"Starting split loader with {len(requests)} requests")
    logger.debug(f"Request details: {requests}")

    tasks = [req.fetch() for req in requests]
    results = await asyncio.gather(*tasks, return_exceptions=return_exceptions)
    logger.info(f"Successfully processed {len(results)} requests in split loader")
    return results
