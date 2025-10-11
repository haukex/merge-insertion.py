"""
Merge-Insertion Sort a.k.a. Ford-Johnson Algorithm
==================================================

The Ford-Johnson algorithm[1], also known as the merge-insertion sort[2,3] uses the minimum
number of possible comparisons for lists of 22 items or less, and at the time of writing has
the fewest comparisons known for lists of 46 items or less. It is therefore very well suited
for cases where comparisons are expensive, such as user input, and the API is implemented to
take an async comparator function for this reason.

>>> from merge_insertion import merge_insertion_sort
>>> # A Comparator must return 0 if the first item is larger, or 1 if the second item is larger.
>>> # It can use any criteria for comparison, in this example we'll use user input:
>>> async def comparator(ab :tuple[str,str]):
...     choice = None
...     while choice not in ab:
...         choice = input(f"Please choose {ab[0]!r} or {ab[1]!r}: ")
...     return 0 if choice == ab[0] else 1
...
>>> # Sort five items in ascending order with a maximum of only seven comparisons:
>>> sorted = merge_insertion_sort('DABEC', comparator)
>>> # Since we can't `await` in the REPL, use asyncio to run the coroutine here:
>>> import asyncio
>>> asyncio.run(sorted)  # doctest: +SKIP
Please choose 'D' or 'A': D
...
Please choose 'B' or 'A': B
['A', 'B', 'C', 'D', 'E']

**References**

1. Ford, L. R., & Johnson, S. M. (1959). A Tournament Problem.
   The American Mathematical Monthly, 66(5), 387-389. <https://doi.org/10.1080/00029890.1959.11989306>
2. Knuth, D. E. (1998). The Art of Computer Programming: Volume 3: Sorting and Searching (2nd ed.).
   Addison-Wesley. <https://cs.stanford.edu/~knuth/taocp.html#vol3>
3. <https://en.wikipedia.org/wiki/Merge-insertion_sort>

API
---

.. autoclass:: merge_insertion.T
    :members:

.. autoclass:: merge_insertion.Comparator
    :members:

.. autofunction:: merge_insertion.merge_insertion_sort

.. autofunction:: merge_insertion.merge_insertion_max_comparisons

Author, Copyright and License
-----------------------------

Copyright © 2025 Hauke Dämpfling (haukex@zero-g.net)

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED “AS IS” AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
"""
from collections.abc import Generator, Sequence, Callable, Awaitable
from typing import TypeVar, Literal
from math import floor, ceil, log2

# NOTICE: This file contains very few code comments because it is a port of
# https://github.com/haukex/merge-insertion.js/blob/main/src/merge-insertion.ts
# Please see that file for detailed code comments and explanations.

#: A type of object that can be compared by a :class:`Comparator` and therefore sorted by
#: :func:`merge_insertion_sort`. Must have sensible support for the equality operators.
T = TypeVar('T')

def _group_sizes() -> Generator[int, None, None]:
    prev :int = 0
    i :int = 1
    while True:
        cur :int = 2**i - prev
        yield cur
        prev = cur
        i += 1

def _make_groups(array :Sequence[T]) -> Sequence[tuple[int, T]]:
    items = list(enumerate(array))
    rv :list[tuple[int, T]] = []
    gen = _group_sizes()
    i :int = 0
    while True:
        size = next(gen)
        group = items[i:i+size]
        group.reverse()
        rv.extend(group)
        if len(group)<size:
            break
        i += size
    return rv

#: A user-supplied function to compare two items.
#: The argument is a tuple of the two items to be compared; they must not be equal.
#: Must return a Promise resolving to 0 if the first item is ranked higher, or 1 if the second item is ranked higher.
Comparator = Callable[[tuple[T, T]], Awaitable[Literal[0, 1]]]

async def _bin_insert_index(array :Sequence[T], item :T, comp :Comparator) -> int:
    if not array:
        return 0
    if item in array:
        raise ValueError("item is already in target array")
    if len(array)==1:
        return 0 if await comp((item,array[0])) else 1
    left, right = 0, len(array)-1
    while left <= right:
        mid = left + floor((right-left)/2)
        if await comp((item, array[mid])):
            right = mid - 1
        else:
            left = mid + 1
    return left

def _ident_find(array :Sequence[T], item :T) -> int:
    for i,e in enumerate(array):
        if e is item:
            return i
    raise IndexError(f"failed to find item {item!r} in array")

async def merge_insertion_sort(array :Sequence[T], comparator :Comparator) -> Sequence[T]:
    """Merge-Insertion Sort (Ford-Johnson algorithm) with async comparison.

    :param array: Array of to sort. Duplicate items are not allowed.
    :param comparator: Async comparison function.
    :return: A shallow copy of the array sorted in ascending order.
    """
    if len(array)<1:
        return []
    if len(array)==1:
        return list(array)
    if len(array) != len(set(array)):
        raise ValueError('array may not contain duplicate items')
    if len(array)==2:
        return list(array) if await comparator((array[0], array[1])) else [array[1], array[0]]

    pairs :dict[T, T] = {}
    for i in range(0, len(array)-1, 2):
        if await comparator((array[i], array[i+1])):
            pairs[array[i+1]] = array[i]
        else:
            pairs[array[i]] = array[i+1]

    larger = await merge_insertion_sort(list(pairs), comparator)

    main_chain :list[list[T]] = [ [ pairs[larger[0]] ], [ larger[0] ] ] + [ [ la, pairs[la] ] for la in larger[1:] ]
    assert all( len(i)==2 for i in main_chain[2:] )

    for _,pair in _make_groups( main_chain[2:] + ( [[array[-1]]] if len(array) % 2 else [] ) ):
        if len(pair)==1:
            item = pair[0]
            idx = await _bin_insert_index([ i[0] for i in main_chain ], item, comparator)
        else:
            assert len(pair)==2
            pair_idx = _ident_find(main_chain, pair)
            item = pair.pop()
            idx = await _bin_insert_index([ i[0] for i in main_chain[:pair_idx] ], item, comparator)
        main_chain.insert(idx, [item])
    assert all( len(i)==1 for i in main_chain )

    return [ i[0] for i in main_chain ]

def merge_insertion_max_comparisons(n :int) -> int:
    """Returns the maximum number of comparisons that :func:`merge_insertion_sort` will perform depending on the input length.

    :param n: The number of items in the list to be sorted.
    :return: The expected maximum number of comparisons.
    """
    if n<0:
        raise ValueError("must specify zero or more items")
    return n*ceil(log2(3*n/4)) - floor((2**floor(log2(6*n)))/3) + floor(log2(6*n)/2) if n else 0
