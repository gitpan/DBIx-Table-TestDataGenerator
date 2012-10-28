package DBIx::Table::TestDataGenerator::TableProbe::SQLite;
use Moo;
use Moo::Role;

use strict;
use warnings;

use Carp;

use DBI qw(:sql_types);
use List::MoreUtils qw ( any );

use DBIx::Table::TestDataGenerator;

use Readonly;
Readonly my $COMMA         => q{,};
Readonly my $QUESTION_MARK => q{?};

with 'DBIx::Table::TestDataGenerator::TableProbe';

sub column_names {
    my ($self) = @_;
    my $sql = <<"END_SQL";
PRAGMA table_info('${\$self->table}')
END_SQL
    my $sth = $self->dbh->prepare($sql);
    $sth->execute();

    my @columns;
    while ( my @row = $sth->fetchrow_array ) {
        push @columns, $row[1];
    }
    return \@columns;
}

sub num_roots {
    my ( $self, $pkey_col, $parent_pkey_col ) = @_;
    my $table = $self->table;

    #note: SQLiteQL ignores NULL when counting values!
    #Therefore we use Coalesce to first replace NULL values by 0
    my $sql = <<"END_SQL";
SELECT COUNT($parent_pkey_col)
FROM $table
WHERE $pkey_col = $parent_pkey_col OR $parent_pkey_col IS NULL
END_SQL
    my $sth = $self->dbh->prepare($sql);
    $sth->execute();
    return ( $sth->fetchrow_array() )[0];
}

sub seed {

    #do nothing, see documentation
    return;
}

sub random_record {
    my ( $self, $table, $colname_list ) = @_;
    my $sql = <<"END_SQL";
SELECT $colname_list
FROM $table
ORDER BY RANDOM() LIMIT 1
END_SQL
    return $self->dbh->selectrow_hashref($sql);
}

{
    my @num_types = qw(INTEGER REAL);
    my @chr_types = qw( TEXT );

    #TODO: no date data type in SQLite, to handle dates one needs to
    #parse columns of type TEXT
    my @date_types = qw();

    sub get_incrementor {
        my ( $self, $type, $max ) = @_;
        if ( any { $type eq $_ } @num_types ) {
            return sub { return ++$max };
        }
        if ( any { $type eq $_ } @chr_types ) {
            my $i      = 0;
            my $suffix = 'A' x $max;
            return sub {
                return $suffix . $i++;
                }
        }
        croak
            "I do not know how to increment unique constraint column of type $type";
    }
}

sub get_type_preference_for_incrementing {
    return [ 'INTEGER', 'REAL', 'TEXT' ];
}

sub unique_columns_with_max {
    my ( $self, $get_pkey_columns ) = @_;

    my $sql;
    my $dbh        = $self->dbh;
    my $table_name = $self->table;

    my %uniq_col_info;

    if ($get_pkey_columns) {

        #the name does not matter, 'pkey' is o.k. since there is
        #only one primary key
        my @pkey_col_names = $dbh->primary_key( undef, undef, $table_name );
        $uniq_col_info{'pkey'} = {} if @pkey_col_names > 0;

        for my $col (@pkey_col_names) {
            my $data_type = $self->_get_data_type( $col, $table_name );
            my $max_val = $self->_get_max( $data_type, $col, $table_name );

            $uniq_col_info{'pkey'}->{$data_type} ||= [];
            push @{ $uniq_col_info{'pkey'}->{$data_type} },
                [ $col, $max_val ];
            last;
        }

    }    # end of primary key handling
    else {

        #unique constraint handling
        my $sth = $self->dbh->prepare("PRAGMA index_list($table_name)");
        $sth->execute();
        while ( my @row = $sth->fetchrow_array() ) {
            my ( $index_name, $is_unique_index ) = ( $row[1], $row[2] );
            next unless $is_unique_index;
            $uniq_col_info{$index_name} = {};

            #determine column names in unique index
            my $sth1 = $self->dbh->prepare("PRAGMA index_info($index_name)");
            $sth1->execute();
            while ( my @row1 = $sth1->fetchrow_array() ) {
                my $col = $row1[2];
                my $data_type = $self->_get_data_type( $col, $table_name );
                my $max_val =
                    $self->_get_max( $data_type, $col, $table_name );

                $uniq_col_info{$index_name}->{$data_type} ||= [];
                push @{ $uniq_col_info{$index_name}->{$data_type} },
                    [ $col, $max_val ];
            }
        }
    }
    return \%uniq_col_info;
}

sub _get_max {
    my ( $self, $data_type, $col, $table_name ) = @_;
    my %max_expr = (
        'INTEGER' => "MAX($col)",
        'REAL'    => "MAX($col)",
        'TEXT'    => "MAX(LENGTH($col))",
    );
    my $max_sql = <<"END_SQL";
SELECT $max_expr{$data_type}
FROM $table_name
END_SQL
    my $max_sth = $self->dbh->prepare($max_sql);
    $max_sth->execute();

    return ( $max_sth->fetchrow_array() )[0];
}

{
    my %data_type = ();

    sub _get_data_type {
        my ( $self, $col_name, $table_name ) = @_;
        unless ( keys %data_type ) {
            my $sth = $self->dbh->prepare("PRAGMA table_info($table_name)");
            $sth->execute();
            while ( my @row = $sth->fetchrow_array() ) {
                $data_type{ $row[1] } = $row[2];
            }
        }
        return $data_type{$col_name};
    }
}

#returns a ref to an array of array refs, where each of the latter
#is the metadata information for a single column constrained by a
#foreign key constraint. The meaning of these elements is as
#follows:
#   0. fkey id
#   1. referencing column id in current fkey
#   2. referenced table name
#   3. referencing column name
#   4. referenced column name
#   5. ON UPDATE action
#   6. ON DELETE action
#   7. match (?), always has value "NONE"
#We are only concerned with columns 0, 2, 3 and 4 here.
sub _get_foreign_key_info {
    my ( $self, $table_name ) = @_;
    my @foreign_key_info;
    my $sth = $self->dbh->prepare("PRAGMA foreign_key_list($table_name)");
    $sth->execute();
    while ( my @row = $sth->fetchrow_array ) {
        push @foreign_key_info, \@row;
    }
    return \@foreign_key_info;
}

sub fkey_name_to_fkey_table {
    my ($self) = @_;
    my $table_name = $self->table;

    my %fkey_tables;

    my @foreign_key_info = @{ $self->_get_foreign_key_info($table_name) };

    #there is a record for each column of each foreign key constraint, so
    #there are duplicates in the following assignment, but this does not
    #hurt
    for my $col_info (@foreign_key_info) {
        $fkey_tables{ @{$col_info}[0] } = @{$col_info}[2];
    }
    return \%fkey_tables;
}

sub fkey_referenced_cols_to_referencing_cols {
    my ($self) = @_;
    my $table_name = $self->table;

    my @foreign_key_info = @{ $self->_get_foreign_key_info($table_name) };

    my %all_refcol_to_col_dict;

    for my $col_info (@foreign_key_info) {
        my ( $fkey, $ref_col, $cons_col ) = @{$col_info}[ 0, 4, 3 ];
        if ( !defined $all_refcol_to_col_dict{$fkey} ) {
            $all_refcol_to_col_dict{$fkey} = {};
        }
        ${ $all_refcol_to_col_dict{$fkey} }{$ref_col} = $cons_col;
    }
    return \%all_refcol_to_col_dict;
}

sub fkey_referenced_cols {
    my ( $self, $fkey_tables ) = @_;

    my @foreign_key_info = @{ $self->_get_foreign_key_info( $self->table ) };

    my %all_refcol_lists;

    foreach ( keys %{$fkey_tables} ) {
        my $fkey = $_;
        my @ref_col_list;

        for my $col_info (@foreign_key_info) {
            my ( $fkey1, $ref_col ) = @{$col_info}[ 0, 4 ];
            next unless $fkey1 eq $fkey;
            push @ref_col_list, $ref_col;
        }
        my @ref_cols = join ', ', @ref_col_list;
        $all_refcol_lists{$fkey} = \@ref_cols;
    }

    return \%all_refcol_lists;
}

sub get_self_reference {
    my ( $self, $fkey_tables, $pkey_col_name ) = @_;
    my $table_name = $self->table;

    my %all_refcol_to_col_dict =
        %{ $self->fkey_referenced_cols_to_referencing_cols() };

    my @self_ref_info;

    for my $fkey ( keys %all_refcol_to_col_dict ) {

        #ignore fkeys pointing to other tables than the target table
        next unless $fkey_tables->{$fkey} eq $table_name;

        #ignore fkeys involving more than one column
        my %dict = %{ $all_refcol_to_col_dict{$fkey} };
        next unless keys %dict == 1;

        #check that name of referenced column is name of primary key column
        if ( ( keys %dict )[0] eq $pkey_col_name ) {
            @self_ref_info = ( $fkey, ( values %dict )[0] );
        }
    }

    return \@self_ref_info;
}

sub selfref_tree {
    my ( $self, $key_col, $parent_refkey_col ) = @_;
    my $table_name = $self->table;
    my $sql        = <<"END_SQL";
SELECT t.$key_col, t1.$key_col
FROM $table_name t LEFT OUTER JOIN $table_name t1
ON t.$parent_refkey_col = t1.$key_col;

END_SQL

    my %tree;
    my $sth = $self->dbh->prepare($sql);
    $sth->execute();
    while ( my ( $id, $parent_id ) = $sth->fetchrow_array() ) {
        if ( defined $tree{$parent_id} ) {
            push @{ $tree{$parent_id} }, $id;
        }
        else {
            $tree{$parent_id} = [$id];
        }
    }
    return \%tree;
}

1;    # End of DBIx::Table::TestDataGenerator::TableProbe::SQLite

__END__

=pod

=head1 NAME

DBIx::Table::TestDataGenerator::TableProbe::SQLite - SQLite (meta)data provider

=head1 DESCRIPTION

This module impersonates the TableProbe role to provide SQLite support.

=head1 SUBROUTINES/METHODS

For general comments about the TableProbe role methods, see the documentation of L<TableProbe|DBIx::Table::TestDataGenerator::TableProbe>.

=head2 seed

The random number generation of SQLite does not provide a method to seed it (yet), so this method does nothing.

=head2 fkey_name_to_fkey_table

In the case of SQLite, the foreign key constraints do not have names, but they have integer ids starting at 0. We use these integers as foreign key names.

=head1 AUTHOR

Jos\x{00E9} Diaz Seng, C<< <josediazseng at gmx.de> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Jos\x{00E9} Diaz Seng.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

