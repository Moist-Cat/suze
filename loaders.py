import json
import csv
from pathlib import Path
import asyncio
import aiohttp
from collections import defaultdict
from typing import Callable

try:
    from yt_dlp import YoutubeDL as yt
except ImportError:
    yt = None

from suze.utils import distance
from suze.log import get_logger


logger = get_logger("loader")


# I/O
# file system
def _sync_read(file_path: Path) -> str:
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
    logger.info(f"Loading text file: {file_path}")
    return await asyncio.to_thread(_sync_read, file_path)


async def json_loader(file_path: Path):
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
    logger.info(f"Loading log file: {file_path}")
    content = await text_loader(file_path)
    lines = content.splitlines()
    logger.debug(f"Loaded {len(lines)} lines from log file {file_path}")
    return lines


# net
async def http_loader(session, url, method, request_data, headers, raw=False):
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
    if not yt:
        raise ImportError("Please install yt-dlp")

    search_query = f"{data['primaryTitle']} {data['startYear']} Official Trailer"

    def sync_yt_loader(search_query=search_query):
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
):
    """
    Merge data from different sources into one.
    It can be used as a search/filtering function as well since it works in-memory.
    If no `target` is selected, no merge is done and the data is transformed into
    a list of named tuples.
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
    master_table = list(data_dict)[0]

    for dep_id, data in data_dict.items():
        logger.debug(f"Processing data from source: {dep_id}")
        if dep_id not in selector:
            logger.debug(
                "Skipping dependency in merge ('%s' is not in selector)", dep_id
            )
            continue

        if not isinstance(data, list):
            data = [data]
            logger.debug(f"Converted non-list data to list with 1 element")

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
                        f"Target field '{target}' not found in record from source {dep_id}"
                    )
                    print(data)
                    raise KeyError(f"Wrong index field for merge: '{target}'")

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
                if dep_id != master_table:
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
                    elif isinstance(merge_strategy, Callable):
                        logger.debug(
                            f"Using callable {merge_strategy} to decide what to do"
                        )
                        merged[field] = merge_strategy(
                            merged[field], value, merged, record
                        )

                    else:
                        logger.error(f"Unknown merge strategy: {merge_strategy}")
                        raise ValueError(f"Unknown merge strategy {merge_strategy}")
                else:
                    merged[field] = value
                    logger.debug(f"Set field '{field}' with new value")

    logger.info(f"Merge completed. Final table has {len(table)} entries")
    return table


async def split_loader(requests):
    """
    Make many slightly different requests
    """
    logger.info(f"Starting split loader with {len(requests)} requests")
    logger.debug(f"Request details: {requests}")

    tasks = [req.fetch() for req in requests]
    results = await asyncio.gather(*tasks)
    logger.info(f"Successfully processed {len(results)} requests in split loader")
    return results
