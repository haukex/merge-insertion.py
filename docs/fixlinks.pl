#!/usr/bin/env perl
use warnings;
use strict;
$^I = ""; # -i command line switch, edit file inplace
my %PFX = (
    '### *class* merge_insertion.T' => 'merge_insertion.T',
    '### merge_insertion.Comparator' => 'merge_insertion.Comparator',
    '### *async* merge_insertion.merge_insertion_sort(' => 'merge_insertion.merge_insertion_sort',
    '### merge_insertion.merge_insertion_max_comparisons(' => 'merge_insertion.merge_insertion_max_comparisons',
);
my ($regex) = map { qr/$_/ } join '|', map {quotemeta} sort { length $b <=> length $a or $a cmp $b } keys %PFX;
while (<>) {
    s{($regex)} {<a id="$PFX{$1}"></a>\n\n$&}g;
} continue { print }
