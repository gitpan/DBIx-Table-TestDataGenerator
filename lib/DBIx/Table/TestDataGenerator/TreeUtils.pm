package DBIx::Table::TestDataGenerator::TreeUtils;
use Moo;

use strict;
use warnings;

use Carp;

has _stack => (
    is       => 'ro',
    default  => sub { return [] },
    init_arg => undef,
);

has _handled => (
    is       => 'ro',
    default  => sub { return [] },
    init_arg => undef,
);

sub _prune_tree {
    my ( $self, $tree_ref, $base ) = @_;
    foreach my $node ( @{ $tree_ref->{$base} } ) {
        $tree_ref = $self->_prune_tree( $tree_ref, $node ) if $node != $base;
    }
    delete $tree_ref->{$base};
    return $tree_ref;
}

{

    sub add_child {
        my ( $self, $tree_ref, $pkey, $min_children, $max_depth ) = @_;
        my %tree = %{$tree_ref};

        if ( @{ $self->_stack } == 0 ) {

            #check if there is a node at level 1
            foreach ( keys %tree ) {
                my $id = $_;
                if ( @{ $tree{$id} } > 0 && $id ~~ @{ $tree{$id} } ) {
                    push @{ $self->_stack }, $id;
                    last;
                }
            }
        }

        #if no node found at level 1, add a new
        #base node, return current $pkey
        if ( @{ $self->_stack } == 0 ) {
            push @{ $self->_stack }, $pkey;
            $tree{$pkey} = [$pkey];
            return [ \%tree, $pkey ];
        }

        #handle children of current node if at a level <= max depth - 1
        #if too few children, add one with id $pkey, return current
        #node id
        if ( @{ $self->_stack } <= $max_depth - 1 ) {

            #check if there are any children at all
            if ( defined $tree{ @{ $self->_stack }[-1] } ) {

                #add a child if not enough children exist.
                #Note that, if current level = 1, we have the
                #current node as one of its own child nodes
                my $num_missing =
                    $min_children
                    - @{ $tree{ @{ $self->_stack }[-1] } }
                    + ( @{ $self->_stack } == 1 ? 1 : 0 );
                if ( $num_missing > 0 ) {
                    push @{ $tree{ @{ $self->_stack }[-1] } }, $pkey;
                    return [ \%tree, @{ $self->_stack }[-1] ];
                }
            }
            else {

                #current node has no children yet, add one
                #and return current node id
                $tree{ @{ $self->_stack }[-1] } = [$pkey];
                return [ \%tree, @{ $self->_stack }[-1] ];
            }
        }

        #o.k., current node has enough children.

        #if level of current node = $max_depth - 1, mark
        #current node as handled, remove from node stack
        if ( @{ $self->_stack } == $max_depth - 1 ) {

            #if current node was base node (only possible if $max_depth = 2),
            #remove it from tree
            if ( @{ $self->_stack } == 1 ) {
                %tree =
                    %{ $self->_prune_tree( \%tree, @{ $self->_stack }[0] ) };
                @{ $self->_stack }   = ();
                @{ $self->_handled } = ();
            }
            else {
                push @{ $self->_handled }, pop @{ $self->_stack };
            }
            return $self->add_child( \%tree, $pkey, $min_children,
                $max_depth );
        }

        #current node is not as deep as $max_depth -1,
        #we need to handle $min_children child nodes of it
        my $handled_children;
        foreach my $curr_child ( @{ $tree{ @{ $self->_stack }[-1] } } ) {

            #exclude self-reference
            next if @{ $self->_stack }[-1] == $curr_child;
            $handled_children++;

            #ignore already handled nodes
            next if $curr_child ~~ @{ $self->_handled };

            #o.k., child not handled yet, make it the current
            #node and recurse
            push @{ $self->_stack }, $curr_child;
            my $parent_key;
            ( $tree_ref, $parent_key ) =
                @{ $self->add_child( \%tree, $pkey, $min_children,
                    $max_depth ) };
            if ( defined $parent_key ) {
                return [ $tree_ref, $parent_key ];
            }
            last if $handled_children == $min_children;
        }

        #all descendants of current node have been handled
        push @{ $self->_handled }, pop @{ $self->_stack };

        #if current node was base node, remove it from tree
        if ( @{ $self->_stack } == 0 ) {
            %tree =
                %{ $self->_prune_tree( \%tree, @{ $self->_handled }[-1] ) };
            @{ $self->_stack }   = ();
            @{ $self->_handled } = ();
        }
        return $self->add_child( \%tree, $pkey, $min_children, $max_depth );
    }
}

1;    # End of DBIx::Table::TestDataGenerator::TreeUtils

__END__

=pod

=head1 NAME

DBIx::Table::TestDataGenerator::TreeUtils - tree builder, used internally to handle self-references in the target table

=head1 DESCRIPTION

This module has nothing to do with databases and could be used on its own. It handles ordered directed graphs, we call them trees here for lack of a better word, but in general they are trees with the root node cut off. The trees are represented as hashes where the keys are seen as parent identifiers and the values are references to arrays containing the child identifiers as elements. It provides a method add_child which adds a node in a place automatically determined and satisfying constraints defined by the parameters passed to add_child. Branches where no more children will be added are removed from the tree in the process.

=head1 SUBROUTINES/METHODS

=head2 _stack

Stack containing the currently handled node and its ancestors, internal use only.

=head2 _handled

Contains the handled nodes, i.e. those not getting any more child nodes, internal use only.

=head2 _prune_tree ( $tree_ref, $base )

Arguments:

=over 4

=item * $tree_ref: Reference to a hash representing a tree structure

=item * $base: Identifier of a root node

=back 

Removes the root node $base and all its descendants from the tree $tree_ref and returns a reference to the pruned tree, internal use only.

=head2 add_child

Arguments:

=over 4

=item * $tree_ref 

Reference to a tree (see above for what we mean by "tree"). 

=item * $pkey 

Identifier of the current child node to be added. 

=item * $min_children

For each handled parent node, this is the minimum number of child nodes to be added. The minimum number may not be reached if the parent node is the last one for which add_child is called.

=item * $max_depth

Maximum depth at which nodes are added, must be at least 2 since all nodes other than root nodes are at least at level 2. The returned result is a pair ($tree_ref, $parent_id), where $tree_ref is a reference to a tree based on the old tree containing an additional child node with identifier $pkey, the tree may have been pruned, too. $parent_id is the identifier of the node at which the child note has been appended. The position of the appended node is determined in a depth-first manner.

=back

=head1 AUTHOR

Jos\x{00E9} Diaz Seng, C<< <josediazseng at gmx.de> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Jos\x{00E9} Diaz Seng.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

