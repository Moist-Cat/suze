"""Transformation utilities for data processing.

This module provides transformation functions for manipulating data structures,
including field transformations, field dropping/renaming, and regex operations.
"""

import re

from suze.log import get_logger


logger = get_logger("transform")


def regex_transformer(
    field: str,
    pattern: str,
    operation: str = "match",
    replacement: str = None,
    group=None,
    flags: int = 0,
):
    """
    A general-purpose regex transformer with multiple operations.

    Args:
        field: Input string to process
        pattern: Regular expression pattern
        operation: Type of operation to perform:
                   "match" - Find first match (default)
                   "search" - Search for pattern anywhere in string
                   "findall" - Find all matches
                   "sub" - Substitute matches with replacement
                   "split" - Split string using pattern as delimiter
                   "groups" - Return all captured groups
                   "span" - Return start and end indices of match
        replacement: Replacement string for "sub" operation
        group: For match/search operations, specify which group to return.
               Can be integer (group number) or string (named group)
        flags: Regex flags (re.IGNORECASE, re.MULTILINE, etc.)

    Returns:
        Transformed string, list of matches, or None if no match found

    Examples:
        >>> regex_transformer("test@example.com", r"@(.+)", "match", group=1)
        'example.com'
        >>> regex_transformer("a,b,c", r",", "split")
        ['a', 'b', 'c']
        >>> regex_transformer("hello world", r"world", "sub", "there")
        'hello there'

    Raises:
        ValueError: If operation is unknown or replacement is missing for 'sub' operation.
    """
    if operation == "match":
        match = re.match(pattern, field, flags)
        if not match:
            return None

        if group is not None:
            if isinstance(group, str):
                return match.group(group)
            return match.group(group)

        return match.group(0) if match else None

    elif operation == "search":
        match = re.search(pattern, field, flags)
        if not match:
            return None

        if group is not None:
            if isinstance(group, str):
                return match.group(group)
            return match.group(group)

        return match.group(0)

    elif operation == "findall":
        return re.findall(pattern, field, flags)

    elif operation == "sub":
        if replacement is None:
            raise ValueError("Replacement string required for 'sub' operation")
        return re.sub(pattern, replacement, field, flags=flags)

    elif operation == "split":
        return re.split(pattern, field, flags=flags)

    elif operation == "groups":
        match = re.search(pattern, field, flags)
        return match.groups() if match else None

    elif operation == "span":
        match = re.search(pattern, field, flags)
        return match.span() if match else None

    else:
        raise ValueError(
            f"Unknown operation: {operation}. "
            f"Must be one of: match, search, findall, sub, split, groups, span"
        )


def field_transform(items, dependencies, target=None, keys=None, transformer=None):
    """
    Adds or updates a field based on other field(s) and a transformer function.

    This function applies a transformation to each item in a list, creating or
    updating a target field with the result of applying the transformer function
    to specified source fields.

    Args:
        items: List of dictionaries representing the data items to transform.
        dependencies: Additional dependencies (unused in this function but
                     maintained for API consistency with other transforms).
        target: Name of the field to create or update with the transformed value.
                If None, no transformation is performed.
        keys: List of field names to use as input to the transformer function.
              The values of these fields will be passed as arguments to the
              transformer in the order specified.
        transformer: Callable function that takes the values of the specified
                    keys as arguments and returns the transformed value.

    Returns:
        List of transformed items with the new/updated target field.

    Examples:
        >>> items = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
        >>> def add(x, y): return x + y
        >>> field_transform(items, None, 'sum', ['a', 'b'], add)
        [{'a': 1, 'b': 2, 'sum': 3}, {'a': 3, 'b': 4, 'sum': 7}]

    Notes:
        - If any item doesn't have all the specified keys, it's skipped.
        - The original items are modified in place.
        - The `dependencies` parameter is part of the transformation function
          signature convention but not used in this implementation.
    """
    if not items or not target or not keys or not transformer:
        return items

    logger.info(
        "Applying a transformation to %d items on field %s using %s with %s",
        len(items),
        target,
        keys,
        transformer,
    )

    for item in items:
        fields = []
        if not set(keys).issubset(set(item.keys())):
            continue
        for key in keys:
            fields.append(item[key])
        item[target] = transformer(*fields)
    return items


def drop_transform(items, dependencies, whitelist=None, blacklist=None):
    """
    Drops fields from items based on whitelist and/or blacklist.

    This function filters the fields in each item, keeping only those specified
    in the whitelist (if provided) and excluding those in the blacklist.
    When both whitelist and blacklist are provided, whitelist takes precedence.

    Args:
        items: List of dictionaries representing the data items to filter.
        dependencies: Additional dependencies (unused in this function but
                     maintained for API consistency with other transforms).
        whitelist: List of field names to keep. If provided, only these fields
                   will be retained in the output. If None or empty, all fields
                   are considered for retention (subject to blacklist).
        blacklist: List of field names to remove. These fields will be excluded
                   from the output. If None or empty, no fields are excluded
                   (subject to whitelist).

    Returns:
        List of items with filtered fields. Items are modified in place.

    Examples:
        >>> items = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}]
        >>> # Keep only 'a' and 'b'
        >>> drop_transform(items, None, whitelist=['a', 'b'])
        [{'a': 1, 'b': 2}, {'a': 4, 'b': 5}]
        >>> # Remove 'c'
        >>> drop_transform(items, None, blacklist=['c'])
        [{'a': 1, 'b': 2}, {'a': 4, 'b': 5}]
        >>> # Whitelist takes precedence when both are provided
        >>> drop_transform(items, None, whitelist=['a'], blacklist=['b'])
        [{'a': 1}, {'a': 4}]

    Notes:
        - If whitelist is provided, blacklist is effectively ignored.
        - The original items are modified in place.
        - The `dependencies` parameter is part of the transformation function
          signature convention but not used in this implementation.
    """
    if not items:
        return items

    if whitelist:
        whitelist = set(whitelist)
        augmented_blacklist = set(items[0].keys()).difference(whitelist)
    else:
        augmented_blacklist = set()

    if not blacklist:
        blacklist = set()
    else:
        blacklist = set(blacklist)

    blacklist = blacklist | augmented_blacklist

    logger.info(
        "Dropping fields %s from %d items",
        blacklist,
        len(items),
    )

    for idx in range(len(items)):
        item = items[idx]
        for key in blacklist:
            if key in item:
                del item[key]

    return items


def rename_transform(items, dependencies, alias_dict=None):
    """
    Renames fields in items according to a mapping dictionary.

    This function renames fields in each item based on the provided alias
    dictionary, where keys are the original field names and values are the
    new field names. Renaming is applied eagerly in the order of the dictionary.

    Args:
        items: List of dictionaries representing the data items to rename.
        dependencies: Additional dependencies (unused in this function but
                     maintained for API consistency with other transforms).
        alias_dict: Dictionary mapping original field names to new field names.
                   If None, no renaming is performed.

    Returns:
        List of items with renamed fields. Items are modified in place.

    Examples:
        >>> items = [{'old_name': 'value1', 'other': 'value2'},
        ...          {'old_name': 'value3', 'other': 'value4'}]
        >>> rename_transform(items, None, {'old_name': 'new_name'})
        [{'new_name': 'value1', 'other': 'value2'},
         {'new_name': 'value3', 'other': 'value4'}]

    Notes:
        - If a field to rename doesn't exist in an item, it's silently skipped.
        - Renaming is applied in the order of the dictionary (Python 3.7+ maintains
          insertion order).
        - The original items are modified in place.
        - The `dependencies` parameter is part of the transformation function
          signature convention but not used in this implementation.
    """
    if alias_dict is None:
        return items

    logger.info(
        "Renaming fields %s from %d items",
        alias_dict,
        len(items),
    )

    for idx in range(len(items)):
        item = items[idx]
        for key, value in alias_dict.items():
            if key not in item:
                continue
            item[value] = item[key]
            del item[key]
    return items
