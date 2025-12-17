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
        regex_transformer("Hello World", r"\w+", "match")  # "Hello"
        regex_transformer("test@example.com", r"@(.+)", "match", group=1)  # "example.com"
        regex_transformer("a,b,c", r",", "split")  # ["a", "b", "c"]
        regex_transformer("hello world", r"world", "sub", "there")  # "hello there"
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
    Adds or updates a field based on other field(s) and a `transform` function
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
    Simple transformation function to drop fields
    """
    if not items:
        return items

    if not blacklist:
        if not whitelist:
            return items
        else:
            blacklist = set(items[0].keys()).difference(set(whitelist))

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
    Simple transformation where we rename field.
    Rename is applied eagerly and in order.
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
