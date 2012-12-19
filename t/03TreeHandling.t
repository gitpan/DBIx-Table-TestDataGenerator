use strict;
use warnings;

use Test::More;

plan tests => 34;

use DBIx::Table::TestDataGenerator::TreeUtils;

my $tree_utils = DBIx::Table::TestDataGenerator::TreeUtils->new();

my $min_children = 3;
my $max_depth    = 4;

sub get_tree {
    my %tree;
    $tree{1} = [ 1, 3, 4 ];
    $tree{2} = [ 2, 5 ];
    $tree{3} = [6];
    $tree{5} = [ 7, 8 ];
    $tree{6} = [ 9, 10, 11 ];
    $tree{7} = [12];
    $tree{12} = [13];
    return \%tree;
}

my $tree_ref = get_tree();

my @expected_parents = (
    1,  3,  3,  15, 15, 15, 16, 16, 16, 4,  4,  4,  23, 23, 23, 24, 24, 24,
    25, 25, 25, 14, 14, 14, 35, 35, 35, 36, 36, 36, 37, 37, 37, 2
);

my $parent_found;
for my $pkey ( 14 .. 47 ) {
    ( $tree_ref, $parent_found ) =
        @{ $tree_utils->add_child( $tree_ref, $pkey, $min_children, $max_depth ) };
    is( $parent_found,
        shift @expected_parents,
        "found correct parent for node with id $pkey"
    );
}
