<a id="module-merge_insertion"></a>

# Merge-Insertion Sort a.k.a. Ford-Johnson Algorithm

The Ford-Johnson algorithm[1], also known as the merge-insertion sort[2,3] uses the minimum
number of possible comparisons for lists of 22 items or less, and at the time of writing has
the fewest comparisons known for lists of 46 items or less. It is therefore very well suited
for cases where comparisons are expensive, such as user input, and the API is implemented to
take an async comparator function for this reason.

```pycon
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
>>> asyncio.run(sorted)  
Please choose 'D' or 'A': D
...
Please choose 'B' or 'A': B
['A', 'B', 'C', 'D', 'E']
```

**References**

1. Ford, L. R., & Johnson, S. M. (1959). A Tournament Problem.
   The American Mathematical Monthly, 66(5), 387-389. <[https://doi.org/10.1080/00029890.1959.11989306](https://doi.org/10.1080/00029890.1959.11989306)>
2. Knuth, D. E. (1998). The Art of Computer Programming: Volume 3: Sorting and Searching (2nd ed.).
   Addison-Wesley. <[https://cs.stanford.edu/~knuth/taocp.html#vol3](https://cs.stanford.edu/~knuth/taocp.html#vol3)>
3. <[https://en.wikipedia.org/wiki/Merge-insertion_sort](https://en.wikipedia.org/wiki/Merge-insertion_sort)>

## API

<a id="merge_insertion.T"></a>

### *class* merge_insertion.T

A type of object that can be compared by a [`Comparator`](#merge_insertion.Comparator) and therefore sorted by
[`merge_insertion_sort()`](#merge_insertion.merge_insertion_sort). Must have sensible support for the equality operators.

alias of TypeVar(‘T’)

<a id="merge_insertion.Comparator"></a>

### merge_insertion.Comparator

A user-supplied function to compare two items.
The argument is a tuple of the two items to be compared; they must not be equal.
Must return a Promise resolving to 0 if the first item is ranked higher, or 1 if the second item is ranked higher.

<a id="merge_insertion.merge_insertion_sort"></a>

### *async* merge_insertion.merge_insertion_sort(array: [Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[T](#merge_insertion.T)], comparator: [Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[T](#merge_insertion.T), [T](#merge_insertion.T)]], [Awaitable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Awaitable)[[Literal](https://docs.python.org/3/library/typing.html#typing.Literal)[0, 1]]]) → [Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[T](#merge_insertion.T)]

Merge-Insertion Sort (Ford-Johnson algorithm) with async comparison.

* **Parameters:**
  * **array** – Array of to sort. Duplicate items are not allowed.
  * **comparator** – Async comparison function.
* **Returns:**
  A shallow copy of the array sorted in ascending order.

<a id="merge_insertion.merge_insertion_max_comparisons"></a>

### merge_insertion.merge_insertion_max_comparisons(n: [int](https://docs.python.org/3/library/functions.html#int)) → [int](https://docs.python.org/3/library/functions.html#int)

Returns the maximum number of comparisons that [`merge_insertion_sort()`](#merge_insertion.merge_insertion_sort) will perform depending on the input length.

* **Parameters:**
  **n** – The number of items in the list to be sorted.
* **Returns:**
  The expected maximum number of comparisons.

## Author, Copyright and License

Copyright © 2025 Hauke Dämpfling ([haukex@zero-g.net](mailto:haukex@zero-g.net))

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
