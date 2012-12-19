package DBIx::Table::TestDataGenerator::TableProbe;
use Moo;

use strict;
use warnings;

use Carp;

use File::Spec;
use File::Basename;
use File::Path qw /rmtree/;
use Cwd qw /abs_path/;

use Readonly;
Readonly my $COMMA         => q{,};
Readonly my $PIPE          => q{|};
Readonly my $QUESTION_MARK => q{?};

use DBI;
use DBIx::RunSQL;
use DBIx::Class::Relationship;
use DBIx::Table::TestDataGenerator::ResultSetWithRandom;
use DBIx::Class::Schema::Loader qw/ make_schema_at /;

#extra dependencies needed
use DBIx::Class::Optional::Dependencies;

has dsn => ( is => 'ro', );

has user => ( is => 'ro', );

has password => ( is => 'ro', );

has on_the_fly_schema_sql => ( is => 'ro', );

has table => ( is => 'ro', );

has _dbh => ( is => 'rw', );

my ( $dbic_schema, $sth_insert );

my %result_classes;

sub _get_handle_to_db_created_from_script {
    my ( $self ) = @_;
    return DBIx::RunSQL->create(
        dsn     => $self->dsn,
        sql     => $self->on_the_fly_schema_sql,
        force   => 1,        
    );
}

sub dump_schema {
    my ( $self ) = @_;

    #make_schema_at disconnects the passed database handle, therefore we pass
    #a clone to it resp. the handle $dbh_for_schema_dump if defined
    my $dbh_for_dump;
    if ( defined $self->on_the_fly_schema_sql ) {
        $self->_dbh(
            $self->_get_handle_to_db_created_from_script(
                $self->on_the_fly_schema_sql
            )
        );
        $dbh_for_dump =
          $self->_get_handle_to_db_created_from_script(
            $self->on_the_fly_schema_sql );
    }
    else {
        $self->_dbh( DBI->connect( $self->dsn, $self->user, $self->password ) );
        
        $dbh_for_dump =
          DBI->connect( $self->dsn, $self->user, $self->password );
    }

    my $attrs = {
        debug          => 1,
        dump_directory => '.',
		quiet          => 1,
    };
    make_schema_at( 'DBIx::Table::TestDataGenerator::DBIC::Schema',
        $attrs, [ sub { $dbh_for_dump }, {} ] );

    #in the current version, make_schema_at removes '.' from @INC, therefore:
    push @INC, '.';
    eval {
        require DBIx::Table::TestDataGenerator::DBIC::Schema;
        DBIx::Table::TestDataGenerator::DBIC::Schema->import();
        1;
    } or do {
        my $error = $@;
        croak $error;
    };

    $dbic_schema =
      DBIx::Table::TestDataGenerator::DBIC::Schema->connect( sub { $self->_dbh }
      );
}

sub _insert_statement {
    my ( $self, $colname_array_ref ) = @_;
    my $all_cols = join $COMMA, @{$colname_array_ref};
    my $placeholders = join $COMMA,
      ($QUESTION_MARK) x ( 0 + @{$colname_array_ref} );
    return
        'INSERT INTO '
      . $self->table
      . " ($all_cols) VALUES ($placeholders)";
}

sub prepare_insert {
    my ( $self, $all_cols ) = @_;
    $sth_insert = $self->_dbh->prepare( $self->_insert_statement($all_cols) );
}

sub execute_insert {
    my ( $self, $all_vals ) = @_;
    $sth_insert->execute( @{$all_vals} );
}

sub commit {
    my ($self) = @_;
    $self->_dbh->commit()
      unless $self->_dbh->{AutoCommit}
      or croak "Could not commit the inserts:\n" . $self->_dbh->errstr;
}

sub num_records {
    my ($self) = @_;
    my $cls = $self->_get_result_class( $self->table );
    return $dbic_schema->resultset($cls)->count;
}

sub print_table {
    my ( $self, $colname_array_ref, $col_width_array_ref ) = @_;
    my $col_list = join $COMMA, @{$colname_array_ref};

    #determine format string
    my $format =
      $PIPE . join( $PIPE, map { "\%${_}s" } @{$col_width_array_ref} ) . $PIPE;

    #print header
    printf $format, @{$colname_array_ref};
    print "\n";
    my $table = $self->table;

    #get data and print it, too
    my $sql = <<"END_SQL";
SELECT $col_list
FROM $table
END_SQL
    my $sth = $self->_dbh->prepare($sql);
    $sth->execute();

    while ( my @row = $sth->fetchrow_array ) {
        printf $format, @row;
        print "\n";
    }
    return;
}

sub _get_result_class {
    my ( $self, $tab ) = @_;

    $tab = uc $tab;

    return $result_classes{$tab} if $result_classes{$tab};

    foreach my $src_name ( $dbic_schema->sources ) {
        my $result_source = $dbic_schema->source($src_name);
        my %src_descr     = %{ $dbic_schema->source($src_name) };
        my $descr         = $src_descr{name};
        $descr = ref($descr) ? ${$descr} : $descr;
        $descr =~ s/^\W//;
        $descr =~ s/\W$//;
        next unless uc $descr eq $tab;
        $result_classes{$tab} = $result_source->result_class;
        return $result_classes{$tab};
    }
    croak 'could not find result class for ' . $tab
      unless $result_classes{$tab};
}

sub _self_ref_condition {
    my ($self) = @_;
    my %col_relations;
    my $result_class = $self->_get_result_class( $self->table );
    foreach my $sname ( $dbic_schema->sources ) {
        my $s = $dbic_schema->source($sname);
        foreach my $rname ( $s->relationships ) {
            my $rel = $s->relationship_info($rname);
            if (   $rel->{class} eq $result_class
                && defined $rel->{attrs}->{is_foreign_key_constraint}
                && $rel->{attrs}->{is_foreign_key_constraint} eq '1' )
            {
                my %cols = %{ $rel->{cond} };
                foreach my $referencing_col ( keys %cols ) {
                    my $referenced_col = $cols{$referencing_col};

                    $referencing_col =~ s/(?:.*\.)?(.+)/$1/;
                    $referenced_col  =~ s/(?:.*\.)?(.+)/$1/;

                    $col_relations{$referencing_col} =
                      { '=' => \$referenced_col };
                }
                return [ $rname, \%col_relations ];
            }
        }
    }
    return;
}

#TODO: remove next comment
#here the old role methods start
sub column_names {
    my ($self) = @_;
    my $cls = $self->_get_result_class( $self->table );

    my @column_names = $cls->columns;
    return \@column_names;
}

sub random_record {
    my ( $self, $tab, $colname_list, $class_name_passed ) = @_;
    my $result_set;
    if ($class_name_passed) {
        $result_set = $dbic_schema->resultset($tab);
    }
    else {
        my $src = $self->_get_result_class($tab);
        $result_set = $dbic_schema->resultset( $src->result_class );
    }

    bless $result_set, 'DBIx::Table::TestDataGenerator::ResultSetWithRandom';
    my %result;

#temporarily commented out until DBIx::Class::Helper::ResultSet::Random has been patched
#my $row = $result_set->rand->single;
    my $row = $self->_rand($result_set)->single;

    #TODO: extract data, put into %result
    foreach ( @{$colname_list} ) {
        $result{$_} = ${ $row->{_column_data} }{$_};
    }
    return \%result;
}

#####START substitute code until DBIx::Class::Helper::ResultSet::Random has been patched####

{

    my %rand_order_by = (
        'DBIx::Class::Storage::DBI::Sybase::MSSQL' => 'NEWID()',
        'DBIx::Class::Storage::DBI::Sybase::Microsoft_SQL_Server::NoBindVars'
          => 'NEWID()',
        'DBIx::Class::Storage::DBI::Sybase::Microsoft_SQL_Server' => 'NEWID()',
        'DBIx::Class::Storage::DBI::Sybase::ASE::NoBindVars'      => 'RAND()',
        'DBIx::Class::Storage::DBI::Sybase::ASE'                  => 'RAND()',
        'DBIx::Class::Storage::DBI::Sybase'                       => 'RAND()',
        'DBIx::Class::Storage::DBI::SQLite'                       => 'RANDOM()',
        'DBIx::Class::Storage::DBI::SQLAnywhere'                  => 'RAND()',
        'DBIx::Class::Storage::DBI::Pg'                           => 'RANDOM()',
        'DBIx::Class::Storage::DBI::Oracle::WhereJoins' => 'dbms_random.value',
        'DBIx::Class::Storage::DBI::Oracle::Generic'    => 'dbms_random.value',
        'DBIx::Class::Storage::DBI::Oracle'             => 'dbms_random.value',
        'DBIx::Class::Storage::DBI::ODBC::SQL_Anywhere' => 'RAND()',
        'DBIx::Class::Storage::DBI::ODBC::Microsoft_SQL_Server' => 'NEWID()',
        'DBIx::Class::Storage::DBI::ODBC::Firebird'             => 'RAND()',
        'DBIx::Class::Storage::DBI::ODBC::ACCESS'               => 'RND()',
        'DBIx::Class::Storage::DBI::mysql::backup'              => 'RAND()',
        'DBIx::Class::Storage::DBI::mysql'                      => 'RAND()',
        'DBIx::Class::Storage::DBI::MSSQL'                      => 'NEWID()',
        'DBIx::Class::Storage::DBI::InterBase'                  => 'RAND()',
        'DBIx::Class::Storage::DBI::Firebird::Common'           => 'RAND()',
        'DBIx::Class::Storage::DBI::Firebird'                   => 'RAND()',
        'DBIx::Class::Storage::DBI::DB2'                        => 'RAND()',
        'DBIx::Class::Storage::DBI::ADO::MS_Jet'                => 'RND()',
        'DBIx::Class::Storage::DBI::ADO::Microsoft_SQL_Server'  => 'NEWID()',
        'DBIx::Class::Storage::DBI::ACCESS'                     => 'RND()',
    );

    #sort keys descending to handle more specific storage classes first
    my @keys_rand_order_by = sort { $b cmp $a } keys %rand_order_by;

    sub _rand_order_by {
        my ( $self, $result_set ) = @_;
        $result_set->result_source->storage->_determine_driver;
        my $storage = $result_set->result_source->storage;

        foreach my $dbms (@keys_rand_order_by) {
            return $rand_order_by{$dbms} if $storage->isa($dbms);
        }

        return 'RAND()';
    }
}

sub _rand {
    my ( $self, $result_set ) = @_;
    my $order_by = $self->_rand_order_by($result_set);

    return $result_set->search( undef, { rows => 1, order_by => \$order_by } );
}

#####END substitute code until DBIx::Class::Helper::ResultSet::Random has been patched####

sub num_roots {
    my ($self) = @_;
    my $cls = $self->_get_result_class( $self->table );

    #find name of foreign key on target table being a self reference
    my $self_ref_cond = $self->_self_ref_condition();
    my $col_relations = @$self_ref_cond[1];

    return $dbic_schema->resultset($cls)->search($col_relations)->count;
}

sub _remove_package_prefix {
    my ( $self, $pck_name ) = @_;
    $pck_name =~ s/(?:.*::)?([^:]+)/$1/;
    return $pck_name;
}

#todo: improve function, e.g. for SQLite there is no datetime data type,
#instead, "text" is used as the data type, this leads to nonsense values
sub unique_columns_with_max {
    my ( $self, $handle_pkey ) = @_;
    my $tab          = $self->table;
    my $result_class = $self->_get_result_class( $self->table );
    my $src          = $dbic_schema->source($result_class);
    my %constraints  = $src->unique_constraints();

    my %unique_with_max;
    foreach my $constraint_name ( keys %constraints ) {
        next
          unless ( $handle_pkey && $constraint_name eq 'primary'
            || !$handle_pkey && $constraint_name ne 'primary' );

        my %constr_info;
        my @cols = @{ $constraints{$constraint_name} };
        foreach my $col_name (@cols) {

            #note: column types are converted to upper case to simplify
            #comparisons later on
            my $col_type = uc ${ $src->column_info($col_name) }{data_type};
            my $is_text  = $self->_is_text($col_type);
            my $col_max;
            if ($is_text) {
                $col_max = $self->_max_length( $col_name, $result_class );
            }
            else {
                $col_max = $self->_max_value( $col_name, $result_class );
            }

            $constr_info{$col_type} ||= [];
            push @{ $constr_info{$col_type} }, [ $col_name, $col_max ];
        }
        $unique_with_max{$constraint_name} = \%constr_info;
    }
    return \%unique_with_max;
}

sub get_incrementor {
    my ( $self, $type, $max ) = @_;
    if ( $self->_is_text($type) ) {
        my $i      = 0;
        my $suffix = 'A' x $max;
        return sub {
            return $suffix . $i++;
          }
    }

    return sub { return ++$max };
}

sub get_type_preference_for_incrementing {
    my @types =
      qw(DECIMAL DOUBLE FLOAT NUMBER NUMERIC REAL BIGINT INTEGER SMALLINT
      TINYINT NVARCHAR2 NVARCHAR LVARCHAR VARCHAR2 VARCHAR LONGCHAR NTEXT
      TEXT);
    return \@types;
}

sub _is_text {
    my ( $self, $col_type ) = @_;
    return $col_type !~ /\b(?:integer|number|numeric|decimal|long)\b/i;
}

sub _max_value {
    my ( $self, $col_name, $result_class ) = @_;
    return $dbic_schema->resultset($result_class)->search()
      ->get_column($col_name)->max();
}

sub _max_length {
    my ( $self, $col_name, $result_class ) = @_;
    my @vals =
      $dbic_schema->resultset($result_class)->search()->get_column($col_name)
      ->func('LENGTH');
    return ( sort { $b <=> $a } @vals )[0];
}

sub fkey_name_to_source {
    my ($self) = @_;
    my %fkey_to_src;
    my $pck_name    = $self->_get_result_class( $self->table );
    my $source_name = $self->_remove_package_prefix($pck_name);
    my $s           = $dbic_schema->source($source_name);
    foreach my $rname ( $s->relationships ) {
        my $rel = $s->relationship_info($rname);
        if ( defined $rel->{attrs}->{is_foreign_key_constraint}
            && $rel->{attrs}->{is_foreign_key_constraint} eq '1' )
        {
            my $src = $self->_remove_package_prefix( $rel->{source} );
            $fkey_to_src{$rname} = $src;
        }
    }

    return \%fkey_to_src;
}

sub fkey_referenced_cols_to_referencing_cols {
    my ($self) = @_;
    my $table = $self->table;
    my %all_refcol_to_col_dict;

    my $src_descr = $self->_get_result_class( $self->table );

    my @fkey_names = keys %{ $self->fkey_name_to_source() };

    foreach (@fkey_names) {
        my $fkey     = $_;
        my $rel_info = $src_descr->relationship_info($fkey);

        my %refcol_to_col_dict;

        my %col_relation = %{ $rel_info->{cond} };
        foreach my $cond ( keys %col_relation ) {
            my $own_col = $col_relation{$cond};
            $own_col =~ s /^self\.//;
            my $ref_col = $cond;
            $ref_col =~ s /^foreign\.//;

            $refcol_to_col_dict{$ref_col} = $own_col;
        }

        $all_refcol_to_col_dict{$fkey} = \%refcol_to_col_dict;
    }

    return \%all_refcol_to_col_dict;
}

sub fkey_referenced_cols {
    my ($self) = @_;
    my %all_refcol_lists;

    my $src_descr = $self->_get_result_class( $self->table );

    my @fkey_names = keys %{ $self->fkey_name_to_source() };

    foreach (@fkey_names) {
        my $fkey     = $_;
        my $rel_info = $src_descr->relationship_info($fkey);

        my @ref_col_list;

        my %col_relation = %{ $rel_info->{cond} };
        foreach my $cond ( keys %col_relation ) {
            my $ref_col = $cond;
            $ref_col =~ s /^foreign\.//;
            push @ref_col_list, $ref_col;
        }
        $all_refcol_lists{$fkey} = \@ref_col_list;
    }

    return \%all_refcol_lists;
}

sub get_self_reference {
    my ( $self, $pkey_col_name ) = @_;

    my $self_ref_cond = $self->_self_ref_condition();
    my ( $fkey_name, $col_relations ) = @$self_ref_cond;
    my %rel              = %{$col_relations};
    my @referencing_cols = keys %rel;
    my %h                = %{ ( values %rel )[0] };
    return [ $fkey_name, ${ ( values %h )[0] } ];
}

sub selfref_tree {
    my ( $self, $pkey_col, $ref_col ) = @_;
    my $cls = $self->_get_result_class( $self->table );
    my $rs  = $dbic_schema->resultset($cls)->search();
    my %tree;

    while ( my $rec = $rs->next() ) {
        my $parent = $rec->get_column($ref_col);
        $tree{$parent} ||= [];
        push @{ $tree{$parent} }, $rec->get_column($pkey_col);
    }
    return \%tree;
}

1;    # End of DBIx::Table::TestDataGenerator::TableProbe

__END__

=pod

=head1 NAME

DBIx::Table::TestDataGenerator::TableProbe - defines roles of DBMS (meta)data handlers

=head1 DESCRIPTION

This class is used internally. For each DBMS to be supported by DBIx::Table::TestDataGenerator, a class must be implemented which impersonates all the roles defined in the current class TableProbe. Note that in the following, we often abbreviate "foreign key" as "fkey".

=head1 SUBROUTINES/METHODS IMPLEMENTED BY TABLEPROBE ITSELF

=head2 dsn

DBI data source name.

=head2 table

Read-only accessor for the name of the table in which the test data will be created.

=head2 dump_schema

Dumps the DBIx::Class schema for the current database to disk.

=head2 _insert_statement

Argument: A reference to an array containing the column names of the target table.

Returns a parameterized insert statement for the target table involving the passed in column names.

=head2 prepare_insert

Argument: A reference to an array containing the column names of the target table.

Prepares the insert statement.

=head2 execute_insert

Argument: A reference to an array containing the values for all columns of the target table in the order they have been passed to prepare_insert.

Executes the insert statement.

=head2 commit

Commits the insert if necessary.

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

Returns a reference to an array of the lower cased column names of the target table in no particular order.

=head2 num_roots

Arguments:

=over 4

=item * $pkey_col: Name of primary key column

=item * $parent_pkey_col: Name of a column in the target table referencing the column $pkey_col by a foreign key constraint

=back

Returns the number of roots in the target table in case a foreign key reference exists linking the referencing column $parent_pkey_col to the primary key column $pkey_col. A record is considered a node if either $pkey_col = $parent_pkey_col or $parent_pkey_col = NULL.

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

=head2 fkey_name_to_source

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

=item * $parent_refkey_col: name of another column referencing the primary key

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
