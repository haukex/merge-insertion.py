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
   The American Mathematical Monthly, 66(5), 387-389. https://doi.org/10.1080/00029890.1959.11989306
2. Knuth, D. E. (1998). The Art of Computer Programming: Volume 3: Sorting and Searching (2nd ed.).
   Addison-Wesley. https://cs.stanford.edu/~knuth/taocp.html#vol3
3. https://en.wikipedia.org/wiki/Merge-insertion_sort

See Also
--------

* JavaScript / TypeScript version: https://www.npmjs.com/package/merge-insertion

* This algorithm in action: https://haukex.github.io/pairrank/ (select "Efficient")

API
---

.. autoclass:: merge_insertion.T

.. autoclass:: merge_insertion.Comparator

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

#: A type of object that can be compared by a :class:`Comparator` and therefore sorted by
#: :func:`merge_insertion_sort`. Must have sensible support for the equality operators.
T = TypeVar('T')

# Helper that generates the group sizes for _make_groups.
def _group_sizes() -> Generator[int, None, None]:
    # <https://en.wikipedia.org/wiki/Merge-insertion_sort>:
    # "... the sums of sizes of every two adjacent groups form a sequence of powers of two."
    # <https://oeis.org/A014113>: a(0) = 0 and if n>=1, a(n) = 2^n - a(n-1).
    prev :int = 0
    i :int = 1
    while True:
        cur :int = 2**i - prev
        yield cur
        prev = cur
        i += 1

# Helper function to group and reorder items to be inserted via binary search.
# See also the description within the code of merge_insertion_sort.
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

#: A user-supplied async function to compare two items.
#: The single argument is a tuple of the two items to be compared; they must not be equal.
#: Must return 0 if the first item is ranked higher, or 1 if the second item is ranked higher.
Comparator = Callable[[tuple[T, T]], Awaitable[Literal[0, 1]]]

# Helper function to insert an item into a sorted array via binary search.
# Returns the index **before** which to insert the new item, e.g. `array.insert(index, item)`
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

# Finds the index of an object in an array by object identity (instead of equality).
def _ident_find(array :Sequence[T], item :T) -> int:
    for i,e in enumerate(array):
        if e is item:
            return i
    raise IndexError(f"failed to find item {item!r} in array")

async def merge_insertion_sort(array :Sequence[T], comparator :Comparator) -> Sequence[T]:
    """Merge-Insertion Sort (Ford-Johnson algorithm) with async comparison.

    :param array: Array to sort. **Duplicate items are not allowed.**
    :param comparator: Async comparison function as described in :class:`Comparator`.
    :return: A shallow copy of the array sorted in ascending order.
    """
    # Special cases and error checking
    if len(array)<1:
        return []
    if len(array)==1:
        return list(array)
    if len(array) != len(set(array)):
        raise ValueError('array may not contain duplicate items')
    if len(array)==2:
        return list(array) if await comparator((array[0], array[1])) else [array[1], array[0]]

    # Algorithm description adapted and expanded from <https://en.wikipedia.org/wiki/Merge-insertion_sort>:
    # 1. Group the items into ⌊n/2⌋ pairs of elements, arbitrarily, leaving one element unpaired if there is an odd number of elements.
    # 2. Perform ⌊n/2⌋ comparisons, one per pair, to determine the larger of the two elements in each pair.
    pairs :dict[T, T] = {}  # keys are the larger items, values the smaller ones
    for i in range(0, len(array)-1, 2):
        if await comparator((array[i], array[i+1])):
            pairs[array[i+1]] = array[i]
        else:
            pairs[array[i]] = array[i+1]

    # 3. Recursively sort the ⌊n/2⌋ larger elements from each pair, creating an initial sorted output sequence
    #    of ⌊n/2⌋ of the input elements, in ascending order, using the merge-insertion sort.
    larger = await merge_insertion_sort(list(pairs), comparator)

    # Build the "main chain" data structure we will use to insert items into (explained a bit more below), while also:
    # 4. Insert at the start of the sorted sequence the element that was paired with
    #    the first and smallest element of the sorted sequence.
    # Note that we know the main chain has at least one item here due to the special cases at the beginning of this function.
    main_chain :list[list[T]] = [ [ pairs[larger[0]] ], [ larger[0] ] ] + [ [ la, pairs[la] ] for la in larger[1:] ]
    assert all( len(i)==2 for i in main_chain[2:] )

    # 5. Insert the remaining ⌈n/2⌉−1 items that are not yet in the sorted output sequence into that sequence,
    #    one at a time, with a specially chosen insertion ordering, as follows:
    #
    # a. Partition the un-inserted elements yᵢ into groups with contiguous indexes.
    #    There are two elements y₃ and y₄ in the first group¹, and the sums of sizes of every two adjacent
    #    groups form a sequence of powers of two. Thus, the sizes of groups are: 2, 2, 6, 10, 22, 42, ...
    # b. Order the un-inserted elements by their groups (smaller indexes to larger indexes), but within each
    #    group order them from larger indexes to smaller indexes. Thus, the ordering becomes:
    #      y₄, y₃, y₆, y₅, y₁₂, y₁₁, y₁₀, y₉, y₈, y₇, y₂₂, y₂₁, ...
    # c. Use this ordering to insert the elements yᵢ into the output sequence. For each element yᵢ,
    #    use a binary search from the start of the output sequence up to but not including xᵢ to determine
    #    where to insert yᵢ.²
    #
    # ¹ My explanation: The items already in the sorted output sequence (the larger elements of each pair) are
    # labeled xᵢ and the yet unsorted (smaller) elements are labeled yᵢ, with i starting at 1. However, due
    # to step 4 above, the item that would have been labeled y₁ has actually already become element x₁, and
    # therefore the element that would have been x₁ is now x₂ and no longer has a paired yᵢ element. It
    # follows that the first paired elements are x₃ and y₃, and so the first unsorted element to be inserted
    # into the output sequence is y₃. Also noteworthy is that if the input had an odd number of elements,
    # the leftover unpaired element is treated as the last yᵢ element.
    #
    # ² In my opinion, this is lacking detail, and this seems to be true for the other two sources (Ford-Johnson
    # and Knuth) as well. So here is my attempt at adding more details to the explanation: The "main chain" is
    # always kept in sorted order, therefore, for each item of the main chain that has an associated `smaller`
    # item, we know that this smaller item must be inserted *before* that main chain item. The problem I see
    # with the various descriptions is that they don't explicitly explain that the insertion process shifts all
    # the indices of the array, and due to the nonlinear insertion order, this makes it tricky to keep track of
    # the correct array indices over which to perform the insertion search. So instead, below, I use a linear
    # search to find the main chain item being operated on each time, which is expensive, but much easier. It
    # should also be noted that the leftover unpaired element, if there is one, gets inserted across the whole
    # main chain as it exists at the time of its insertion - it may not be inserted last. So even though there
    # is still some optimization potential, this algorithm is used in cases where the comparisons are much more
    # expensive than the rest of the algorithm, so the cost is acceptable for now.

    # Iterate over the groups to be inserted, which are built from the main chain as explained above (in the
    # current implementation we don't need the original indices returned by _make_groups). Also, if there was
    # a leftover item from an odd input length, treat it as the last "smaller" item. We'll use the fact that
    # at this point, all main_chain items contain two elements, so we'll mark the leftover item as a special
    # case by having it be the only item with one element.
    for _,pair in _make_groups( main_chain[2:] + ( [[array[-1]]] if len(array) % 2 else [] ) ):
        # Determine which item to insert and where.
        if len(pair)==1:  # See explanation of this special case above.
            # This is the leftover item, it gets inserted into the current whole main chain.
            item = pair[0]
            idx = await _bin_insert_index([ i[0] for i in main_chain ], item, comparator)
        else:
            assert len(pair)==2
            # Locate the pair we're about to insert in the main chain, to limit the extent of the binary search (see also explanation above).
            pair_idx = _ident_find(main_chain, pair)
            item = pair.pop()
            # Locate the index in the main chain where the pair's smaller item needs to be inserted.
            idx = await _bin_insert_index([ i[0] for i in main_chain[:pair_idx] ], item, comparator)
        # Actually do the insertion.
        main_chain.insert(idx, [item])
    assert all( len(i)==1 for i in main_chain )

    # Turn the "main chain" data structure back into an array of values.
    return [ i[0] for i in main_chain ]

def merge_insertion_max_comparisons(n :int) -> int:
    """Returns the maximum number of comparisons that :func:`merge_insertion_sort` will perform depending on the input length.

    :param n: The number of items in the list to be sorted.
    :return: The expected maximum number of comparisons.
    """
    if n<0:
        raise ValueError("must specify zero or more items")
    # Formula from https://en.wikipedia.org/wiki/Merge-insertion_sort (the sum version should work too)
    return n*ceil(log2(3*n/4)) - floor((2**floor(log2(6*n)))/3) + floor(log2(6*n)/2) if n else 0
