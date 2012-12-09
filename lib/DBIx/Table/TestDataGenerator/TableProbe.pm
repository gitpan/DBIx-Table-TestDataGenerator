package DBIx::Table::TestDataGenerator::TableProbe;
use Moo::Role;

use strict;
use warnings;

use Carp;

use Readonly;
Readonly my $COMMA         => q{,};
Readonly my $PIPE          => q{|};
Readonly my $QUESTION_MARK => q{?};

#Methods that need to be implemented by classes impersonating the
#TableProbe role:
requires qw(seed column_names random_record
    num_roots get_type_preference_for_incrementing get_incrementor
    unique_columns_with_max fkey_name_to_fkey_table
    fkey_referenced_cols_to_referencing_cols
    fkey_referenced_cols get_self_reference selfref_tree);

has dbh => ( is => 'ro', );

has database => ( is => 'ro', );

has schema => ( is => 'ro', );

has table => ( is => 'ro', );

sub insert_statement {
    my ( $self, $colname_array_ref ) = @_;
    my $all_cols = join $COMMA, @{$colname_array_ref};
    my $placeholders = join $COMMA,
        ($QUESTION_MARK) x ( 0 + @{$colname_array_ref} );
    return
          'INSERT INTO '
        . $self->table
        . " ($all_cols) VALUES ($placeholders)";
}

sub num_records {
    my ($self) = @_;
    my $table  = $self->table;
    my $sql    = <<"END_SQL";
SELECT COUNT (*) AS num_records_orig
FROM $table
END_SQL
    my $sth = $self->dbh->prepare($sql);
    $sth->execute();
    return ( $sth->fetchrow_array() )[0];
}

sub print_table {
    my ( $self, $colname_array_ref, $col_width_array_ref ) = @_;
    my $col_list = join $COMMA, @{$colname_array_ref};

    #determine format string
    my $format =
          $PIPE
        . join( $PIPE, map {"\%${_}s"} @{$col_width_array_ref} )
        . $PIPE;

    #print header
    printf $format, @{$colname_array_ref};
    print "\n";
    my $table = $self->table;

    #get data and print it, too
    my $sql = <<"END_SQL";
SELECT $col_list
FROM $table
END_SQL
    my $sth = $self->dbh->prepare($sql);
    $sth->execute();

    while ( my @row = $sth->fetchrow_array ) {
        printf $format, @row;
        print "\n";
    }
    return;
}

1;    # End of DBIx::Table::TestDataGenerator::TableProbe

__END__

=pod

=head1 NAME

DBIx::Table::TestDataGenerator::TableProbe - defines roles of DBMS (meta)data handlers

=head1 DESCRIPTION

This class is used internally. For each DBMS to be supported by DBIx::Table::TestDataGenerator, a class must be implemented which impersonates all the roles defined in the current class TableProbe. Note that in the following, we often abbreviate "foreign key" as "fkey".

=head1 SUBROUTINES/METHODS IMPLEMENTED BY TABLEPROBE ITSELF

=head2 dbh

Read-only accessor for a DBI database handle.

=head2 database

Read-only accessor for a database name.

=head2 schema

Read-only accessor for a database schema name.

=head2 table

Read-only accessor for the name of the table in which the test data will be created.

=head2 insert_statement

Argument: A reference to an array containing the column names of the target table.

Returns a parameterized insert statement for the target table involving the passed in column names.

=head2 num_records

Returns the number of records in the target table.

=head2 print_table

Arguments:

=over 4

=item * $colname_array_ref Reference to an array of column names of the target table.

=item * $col_width_array_ref Reference to an array of integers

=back

Minimalistic printing of a table's contents to STDOUT for debugging purposes. Prints the values for the column in $colname_array_ref in columns whose width is defined by  $col_width_array_ref.

=head1 SUBROUTINES/METHODS TO BE IMPLEMENTED BY CLASSES IMPERSONATING THE TABLEPROBE ROLE

=head2 column_names

Returns a reference to an array of the column names of the target table in no particular order.

=head2 num_roots

Arguments:

=over 4

=item * $pkey_col: Name of primary key column

=item * $parent_pkey_col: Name of a column in the target table referencing the column $pkey_col by a foreign key constraint

=back

Returns the number of roots in the target table in case a foreign key reference exists linking the referencing column $parent_pkey_col to the primary key column $pkey_col. A record is considered a node if either $pkey_col = $parent_pkey_col or $parent_pkey_col = NULL.

=head2 seed

Argument: random number seed $seed.

Seeds the random number with the integer $seed to allow for reproducible runs. In cases such as SQLite, the random number generation cannot be seeded, so the method will do nothing in these cases.

=head2 random_record

=over 4

=item * $table: Table name

=item * $colname_list: Reference to an array containing a subset of the column names of table $table.

=back

Determines a random record from table $table and returns a reference to a hash where the keys are the column names in list $colname_list and the values the values of these columns in the selected record.

=head2 get_incrementor

Arguments:

=over 4

=item * $type: data type of the current DBMS applicable to table columns

=item * $max: value based on which the incrementor will determine the next "incremented" value

=back

Returns an anonymous function for incrementing the values of a column of data type $type starting at a value to be determined by the current "maximum" $max. In case of a numeric data type, $max will be just the current maximum, but in case of strings, we have decided to pass the maximum length since there is no natural ordering available. E.g. Perl using per default another order than the lexicographic order employed by Oracle. In our default implementations, for string data types we add values for the current column at 'A...A0', where A is repeated $max times and increase the appended integer in each step. This should be made more flexible in future versions.

=head2 get_type_preference_for_incrementing 

Arguments: none

We must decide which of the column values of a record to be added will be changed in case of a uniqueness constraint. This method returns a reference to an array listing the supported data types. The order of the data types defines which column in such a unique constraint will get preference over others based on its data type.

=head2 unique_columns_with_max

Argument $get_pkey_columns: used for its falsey or truthy value

In case $get_pkey_columns is false, this method returns a hash reference of the following structure:

  {
      UNIQUE_CONSTR_1 =>
      {
        DATA_TYPE_1 => [ [ COL_NAME_1, MAX_VAL_1 ], ..., [COL_NAME_N, MAX_VAL_N] ],
        DATA_TYPE_2 => [ [ COL_NAME_N+1, MAX_VAL_N+1 ], ..., [COL_NAME_M, MAX_VAL_M] ],
        ...
      }
      UNIQUE_CONSTR_2 => {...}
    ...
  }

Here, the keys of the base hash are the names of all uniqueness constraints. For each such constraint, the value of the base hash is another hash having as values all the data types used for columns in the constraint and as values an array reference where each element is a pair (column_name, max_value) where column_name runs over all column names in the constraint and max_value is the corresponding current maximum value. (Please note the comment in the description of get_incrementor on how we currently determine this maximum in case of string data types.)

In case $get_pkey_columns is true, the corresponding information is returned for the primary key constraint, in particular the base hash has only one key as there may be only one primary key constraint:

  {
      PRIMARY_KEY_NAME =>
      {
        DATA_TYPE_1 => [ [ COL_NAME_1, MAX_VAL_1 ], ..., [COL_NAME_N, MAX_VAL_N] ],
        DATA_TYPE_2 => [ [ COL_NAME_N+1, MAX_VAL_N+1 ], ..., [COL_NAME_M, MAX_VAL_M] ],
        ...
      }
  }

=head2 fkey_name_to_fkey_table

Arguments: none

Returns a hash where the keys are the names of the foreign keys on the target table and the values the names of the corresponding referenced tables.

=head2 fkey_referenced_cols_to_referencing_cols

Arguments: none

Returns a reference to a dictionary having as keys the fkey names and for each key as value a dictionary where the keys are the names of the referenced column names and the values the names of the corresponding referencing column names.

=head2 fkey_referenced_cols

Arguments: none

Returns a reference to a hash having the fkey names as keys and a comma-separated list of the column names of the referenced columns of the fkey as values.

=head2 get_self_reference

Arguments: none

If there is an fkey defining a self-reference, its name and the name of the referencing column are returned in a two-element array reference, otherwise undef is returned.

=head2 selfref_tree

Arguments: 

=over 4

=item * $key_col: primary key column name of a one-column primary key

=item * $parent_refkey_col: name of another column

=back

Suppose we have a self-reference in the target table, i.e. a one-column foreign key pointing to the primary key of the target table. In this case, selfref_tree returns a tree defined by the parent-child relation the self-reference defines. $key_col is the name of the primary key column and $parent_refkey_col the name of the column containing the reference to the parent record.

=head1 AUTHOR

Jose Diaz Seng, C<< <josediazseng at gmx.de> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Jose Diaz Seng.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

