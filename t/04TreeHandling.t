use strict;
use warnings;

use Test::More;

#corner case: test max_depth = 2, this has been buggy for a while (if first root
#found already had enough children, it was removed from the stack managed by
#TreeUtils but not pruned from the tree, so in the next step it was again introduced
#in the stack, resulting in an infinite loop)

plan tests => 5;

use DBIx::Table::TestDataGenerator::TreeUtils;

my $tree_utils = DBIx::Table::TestDataGenerator::TreeUtils->new();

my $max_depth    = 2;
my $min_children = 2;

sub get_tree {
    my %tree;
    $tree{1} = [ 1, 2, 3 ];
    $tree{4} = [ 4, 5 ];
    return \%tree;
}

my $tree_ref = get_tree();

my @expected_parents = ( 4, 7, 7, 7, 10 );

my $parent_found;
for my $pkey ( 6 .. 10 ) {
    ( $tree_ref, $parent_found ) =
        @{ $tree_utils->add_child( $tree_ref, $pkey, $min_children, $max_depth ) };
    is( $parent_found,
        shift @expected_parents,
        "found correct parent for node with id $pkey"
    );
}
