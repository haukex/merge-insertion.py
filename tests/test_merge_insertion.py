"""
Tests for merge-insertion.py

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
import random
import unittest
from typing import Literal, Optional
from itertools import islice, permutations
from collections import defaultdict
import merge_insertion as uut

# NOTICE: This file contains very few code comments because it is essentially a port of
# https://github.com/haukex/merge-insertion.js/blob/main/src/__tests__/merge-insertion.test.ts
# Please see that file for code comments.

async def _comp(ab :tuple[str,str]) -> Literal[0,1]:
    return 0 if ab[0] > ab[1] else 1

class AlwaysEqual:
    def __eq__(self, other):
        return isinstance(other, AlwaysEqual)

class TestMergeInsertionSort(unittest.IsolatedAsyncioTestCase):

    def _test_comp(self, comp :uut.Comparator, max_calls :int, log :Optional[list[tuple[uut.T,uut.T]]] = None) -> uut.Comparator:
        call_count = 0
        pair_map :dict[uut.T, set[uut.T]] = defaultdict(set)
        def c(ab :tuple[uut.T,uut.T]):
            nonlocal call_count
            a, b = ab
            self.assertNotEqual(a, b, f"a and b may not be equal ({a!r})")
            self.assertFalse( a in pair_map and b in pair_map[a] or b in pair_map and a in pair_map[b],
                f"duplicate comparison of {a!r} and {b!r}" )
            pair_map[a].add(b)
            call_count += 1
            self.assertLessEqual(call_count, max_calls, f"too many Comparator calls ({call_count})")
            if log is not None:
                log.append(ab)
            return comp(ab)
        return c

    def test_group_sizes(self):
        group_sizes = uut._group_sizes  # pyright: ignore[reportPrivateUsage]  # pylint: disable=protected-access
        exp = [ 2, 2, 6, 10, 22, 42, 86, 170, 342, 682, 1366, 2730, 5462, 10922, 21846, 43690,
            87382, 174762, 349526, 699050, 1398102, 2796202, 5592406, 11184810, 22369622, 44739242,
            89478486, 178956970, 357913942, 715827882, 1431655766, 2863311530, 5726623062, 11453246122 ]
        self.assertEqual( exp, list( islice( group_sizes(), len(exp) ) ) )

    def test_make_groups(self):
        make_groups = uut._make_groups  # pyright: ignore[reportPrivateUsage]  # pylint: disable=protected-access
        self.assertEqual( make_groups(['y3','y4','y5','y6','y7','y8','y9','y10','y11','y12','y21','y22']),
            [ (1,'y4'), (0,'y3'),  (3,'y6'), (2,'y5'), (9,'y12'), (8,'y11'), (7,'y10'), (6,'y9'), (5,'y8'), (4,'y7'),  (11,'y22'), (10,'y21') ] )
        self.assertEqual( make_groups(['b2','b3','b4','b5','b6','b7','b8','b9','b10','b11']),
            [ (1,'b3'), (0,'b2'),  (3,'b5'), (2,'b4'), (9,'b11'), (8,'b10'), (7,'b9'), (6,'b8'), (5,'b7'), (4,'b6') ] )
        self.assertEqual( make_groups(['1','2','3','4','5','6','7','8','9']),
            [ (1,'2'), (0,'1'),  (3,'4'), (2,'3'), (8,'9'), (7,'8'), (6,'7'), (5,'6'), (4,'5') ] )

    async def test_test_comp(self):
        log :list[tuple[str,str]] = []
        c = self._test_comp(_comp, 2, log)
        await c(('x','y'))
        await c(('x','z'))
        with self.assertRaisesRegex(AssertionError, 'may not be equal'):
            await c(('x','x'))
        with self.assertRaisesRegex(AssertionError, 'duplicate comparison'):
            await c(('y','x'))
        with self.assertRaisesRegex(AssertionError, 'duplicate comparison'):
            await c(('x','y'))
        with self.assertRaisesRegex(AssertionError, 'duplicate comparison'):
            await c(('x','z'))
        with self.assertRaisesRegex(AssertionError, 'too many'):
            await c(('i','j'))
        self.assertEqual( log, [('x','y'), ('x','z')] )

    async def test_bin_insert_index(self):
        bin_insert_index = uut._bin_insert_index  # pyright: ignore[reportPrivateUsage]  # pylint: disable=protected-access

        #              A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
        a :list[str] = ['B','D','F','H','J','L','N','P','R','T','V','X','Z']

        with self.assertRaises(ValueError):
            await bin_insert_index(a, 'J', _comp)

        self.assertEqual( await bin_insert_index([],    'A', self._test_comp(_comp,0)), 0 )
        self.assertEqual( await bin_insert_index(a[:1], 'A', self._test_comp(_comp,1)), 0 )
        self.assertEqual( await bin_insert_index(a[:1], 'C', self._test_comp(_comp,1)), 1 )

        self.assertEqual( await bin_insert_index(a[:2], 'A', self._test_comp(_comp,1)), 0 )
        self.assertEqual( await bin_insert_index(a[:2], 'C', self._test_comp(_comp,2)), 1 )
        self.assertEqual( await bin_insert_index(a[:2], 'E', self._test_comp(_comp,2)), 2 )

        self.assertEqual( await bin_insert_index(a[:5], 'A', self._test_comp(_comp,2)), 0 )
        self.assertEqual( await bin_insert_index(a[:5], 'C', self._test_comp(_comp,3)), 1 )
        self.assertEqual( await bin_insert_index(a[:5], 'E', self._test_comp(_comp,3)), 2 )
        self.assertEqual( await bin_insert_index(a[:5], 'G', self._test_comp(_comp,2)), 3 )
        self.assertEqual( await bin_insert_index(a[:5], 'I', self._test_comp(_comp,3)), 4 )
        self.assertEqual( await bin_insert_index(a[:5], 'K', self._test_comp(_comp,3)), 5 )
        self.assertEqual( await bin_insert_index(a[:5], 'M', self._test_comp(_comp,3)), 5 )

        self.assertEqual( await bin_insert_index(a[:6], 'A', self._test_comp(_comp,2)), 0 )
        self.assertEqual( await bin_insert_index(a[:6], 'G', self._test_comp(_comp,3)), 3 )
        self.assertEqual( await bin_insert_index(a[:6], 'M', self._test_comp(_comp,3)), 6 )

        self.assertEqual( await bin_insert_index(a[:7], 'A', self._test_comp(_comp,3)), 0 )
        self.assertEqual( await bin_insert_index(a[:7], 'G', self._test_comp(_comp,3)), 3 )
        self.assertEqual( await bin_insert_index(a[:7], 'O', self._test_comp(_comp,3)), 7 )

    def test_ident_find(self):
        ident_find = uut._ident_find  # pyright: ignore[reportPrivateUsage]  # pylint: disable=protected-access
        o = AlwaysEqual()
        a = (1, 'foo', AlwaysEqual(), o)
        self.assertEqual( a.index(AlwaysEqual()), 2 )
        self.assertEqual( a.index(o), 2 )
        self.assertEqual( ident_find(a, o), 3 )
        with self.assertRaises(IndexError):
            ident_find(a, AlwaysEqual())

    async def test_merge_insertion_sort_detail(self):
        log :list[tuple[str,str]] = []
        self.assertEqual( await uut.merge_insertion_sort('ABCDE', self._test_comp(_comp, 7, log)), ['A','B','C','D','E'] )
        self.assertEqual( log, [ ('A','B'), ('C','D'), ('B','D'), ('E','B'), ('E','D'), ('C','A'), ('C','B') ] )

        with self.assertRaises(ValueError):
            await uut.merge_insertion_sort('ABB', _comp)

    async def test_merge_insertion_sort_lengths(self):
        random.seed(123)
        for ln in range(95):
            array = [ chr(x+33) for x in range(ln) ]
            a = array[:]
            self.assertEqual( await uut.merge_insertion_sort(a, self._test_comp(_comp, uut.merge_insertion_max_comparisons(ln))), array )
            if ln<2:
                continue
            a.reverse()
            self.assertEqual( await uut.merge_insertion_sort(a, self._test_comp(_comp, uut.merge_insertion_max_comparisons(ln))), array )
            for _ in range(10):
                random.shuffle(a)
                self.assertEqual( await uut.merge_insertion_sort(a, self._test_comp(_comp, uut.merge_insertion_max_comparisons(ln))), array )

    async def test_merge_insertion_sort_permutations(self):
        for ln in range(9):  # don't increase! (runtime)
            array = [ chr(x+65) for x in range(ln) ]
            for perm in permutations(array):
                self.assertEqual( await uut.merge_insertion_sort(perm, self._test_comp(_comp, uut.merge_insertion_max_comparisons(ln))), array )

    def test_merge_insertion_max_comparisons(self):
        exp = [ 0, 0, 1, 3, 5, 7, 10, 13, 16, 19, 22, 26, 30, 34, 38, 42, 46, 50, 54, 58, 62, 66,
            71, 76, 81, 86, 91, 96, 101, 106, 111, 116, 121, 126, 131, 136, 141, 146, 151, 156, 161,
            166, 171, 177, 183, 189, 195, 201, 207, 213, 219, 225, 231, 237, 243, 249, 255 ]
        for i,e in enumerate(exp):
            self.assertEqual( uut.merge_insertion_max_comparisons(i), e )
        with self.assertRaises(ValueError):
            uut.merge_insertion_max_comparisons(-1)
