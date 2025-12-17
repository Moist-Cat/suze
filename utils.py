from functools import cache


@cache
def distance(str1, str2, m=0, n=0):
    """
    Modified version of levenshtein distance.
    Returns how far is str1 from becoming a prefix of str2.
    """
    # str1 is full
    if m == len(str1):
        # NOTE We return 0 here because
        # we don't care if the rest of the str2 doesn't match
        return 0
    # str2 is full
    if n == len(str2):
        return len(str1) - m

    if str1[m] == str2[n]:
        return distance(str1, str2, m + 1, n + 1)

    return 1 + min(
        # Insert
        distance(str1, str2, m, n + 1),
        min(
            # Remove
            distance(str1, str2, m + 1, n),
            # Replace
            distance(str1, str2, m + 1, n + 1),
        ),
    )
