from functools import cache


def rolling_hash(s, base=31, mod=2**61 - 1):
    h = 0
    for c in s:
        h = (h * base + ord(c)) % mod
    return h


@cache
def distance(str1, str2, m=0, n=0):
    """
    Calculates the minimum edit distance to make str1 a prefix of str2.

    This is a modified version of Levenshtein distance that measures how many
    edit operations (insertions, deletions, substitutions) are required to
    transform str1 into a prefix of str2. Unlike standard Levenshtein distance,
    this algorithm doesn't penalize unmatched characters at the end of str2.

    Args:
        str1: The source string to transform into a prefix.
        str2: The target string that str1 should become a prefix of.
        m: Current position in str1 (used for recursion, default 0).
        n: Current position in str2 (used for recursion, default 0).

    Returns:
        int: Minimum number of edit operations required to make str1 a prefix of str2.

    Examples:
        >>> distance("cat", "catch")
        0  # "cat" is already a prefix of "catch"
        >>> distance("catch", "cat")
        2  # Need to delete 'c' and 'h' from "catch" to get "cat" as prefix
        >>> distance("kitten", "sitting")
        3  # Replace 'k' with 's', replace 'e' with 'i', insert 'g'

    Notes:
        - Uses memoization via @cache decorator for performance.
        - Edit operations are weighted equally (cost of 1 each).
        - When str1 is fully processed, returns 0 regardless of remaining str2.
        - When str2 is fully processed but str1 isn't, returns remaining length of str1.
        - Time complexity: O(m*n) with memoization, where m=len(str1), n=len(str2).
        - Space complexity: O(m*n) for the memoization cache.
    """
    # Base case 1: Entire str1 has been processed
    if m == len(str1):
        # We return 0 here because we don't care if the rest of str2 doesn't match.
        # Once str1 is fully processed, it's already a valid prefix.
        return 0

    # Base case 2: Entire str2 has been processed but str1 isn't finished
    if n == len(str2):
        # Need to delete all remaining characters from str1
        return len(str1) - m

    # Characters match - move to next position in both strings
    if str1[m] == str2[n]:
        return distance(str1, str2, m + 1, n + 1)

    # Characters don't match - consider all edit operations
    return 1 + min(
        # Insert operation: insert a character from str2 into str1
        # (skip current character in str2, keep same position in str1)
        distance(str1, str2, m, n + 1),
        min(
            # Delete operation: delete current character from str1
            # (skip current character in str1, keep same position in str2)
            distance(str1, str2, m + 1, n),
            # Replace operation: replace current character in str1 with str2's character
            # (move forward in both strings)
            distance(str1, str2, m + 1, n + 1),
        ),
    )
