package DBIx::Table::TestDataGenerator;
use Moo;

use strict;
use warnings;

our $VERSION = "0.003";
$VERSION = eval $VERSION;

use Carp;
use List::Util qw (first);
use Class::Load qw (load_class);
use DBIx::Table::TestDataGenerator::DBDriverUtils;
use DBIx::Table::TestDataGenerator::TreeUtils;

has dbh => (
    is       => 'ro',
    required => 1
);

has schema => (
    is       => 'ro',
    required => 0
);

has table => (
    is       => 'ro',
    required => 1
);

has custom_probe_class => (
    is       => 'ro',
    required => 0
);

sub create_testdata {
    my $self = shift;
    my %args = @_;

    my $target_size    = $args{target_size};
    my $num_random     = $args{num_random};
    my $seed           = $args{seed};
    my $max_tree_depth = $args{max_tree_depth};
    my $min_children   = $args{min_children};
    my $min_roots      = $args{min_roots};

    my $tree_utils = DBIx::Table::TestDataGenerator::TreeUtils->new();
    my $db_driver_utils =
        DBIx::Table::TestDataGenerator::DBDriverUtils->new();

    $db_driver_utils->check_db_handle( $self->dbh );
    my $database = $db_driver_utils->get_database( $self->dbh );

    #determine the TableProbe subclass for the current DBMS
    my $probe_class = $self->custom_probe_class
        || 'DBIx::Table::TestDataGenerator::TableProbe::'
        . $db_driver_utils->db_driver_name( $self->dbh );

    load_class($probe_class);

    #$probe will be used to determine (meta)data of our target table
    my $probe = $probe_class->new(
        dbh      => $self->dbh,
        database => $database,
        schema   => $self->schema,
        table    => $self->table
    );

    #seed Perl and database random number generation
    if ( defined $seed ) {
        croak "seed should be an integer, which $seed is not"
            unless $seed =~ /^\d+$/;
        srand $seed;
        $probe->seed($seed);
    }

    my ( $num_records_added, $num_roots );

    #TODO: check why $num_random = 1 is not allowed
    if ( $num_random < 2 ) {
        croak 'num_random must be greater or equal to two';
    }

    #In case of a self-reference, new records will be children of existing
    #ones since we follow all foreign keys, in particular those defining
    #a self-reference. The new records will have level >= 2, therefore we
    #exit in case $max_tree_depth is smaller.
    if ( defined $max_tree_depth && $max_tree_depth < 2 ) {
        croak 'max_tree_depth must be greater or equal to two';
    }

    #Exit if only part of the parameters used to handle self-references has
    #been provided.
    if (   defined $max_tree_depth
        || defined $min_children
        || defined $min_roots )
    {
        croak
            'to handle a self-reference, you need to specify max_tree_depth, '
            . 'min_children and min_roots, the min_roots parameter is missing'
            unless defined $min_roots;
        croak
            'to handle a self-reference, you need to specify max_tree_depth, '
            . 'min_children and min_roots, the min_children parameter is missing'
            unless defined $min_children;
        croak
            'to handle a self-reference, you need to specify max_tree_depth, '
            . 'min_children and min_roots, the max_tree_depth parameter is missing'
            unless defined $max_tree_depth;
    }

    #Determine whether the user has provided all informations needed to handle
    #a possible self-reference.
    my $handle_self_ref_wanted =
           defined $max_tree_depth
        && defined $min_children
        && defined $min_roots;

    #Determine original number of records in target table.
    my $num_records_orig = $probe->num_records();

    if ( $num_records_orig == 0 ) {
        croak 'The target table ' . $self->table . ' must not be empty';
    }

    my $num_records_to_insert = $target_size - $num_records_orig;
    if ( $num_records_to_insert <= 0 ) {
        print 'already enough records in table '
            . $self->table
            . "\ncurrent number: $num_records_orig, requested: $target_size\n";
        return 0;
    }

    #Columns whose name does NOT appear in @handled_columns will get
    #their values from the target table itself.
    my @handled_columns;

    ###HANDLE COLUMNS IN UNIQUE CONSTRAINTS###

    #First, get information about the columns in unique constraints.
    my %unique_cols_info = %{ $probe->unique_columns_with_max(0) };
    my %unique_cols_to_incr;

    #Next, for each unique constraint we determine a column whose value will
    #be incremented on each insert into the target table. The TableProbe class
    #influences which column will be selected by defining an order on data
    #types.
    #For the selected column, a (data type dependent) incrementor is provided
    #by the TableProbe class.
    my $type_preference_for_incrementing =
        $probe->get_type_preference_for_incrementing();

    for my $constraint_name ( keys %unique_cols_info ) {
        my %constraint_info = %{ $unique_cols_info{$constraint_name} };
        my $selected_data_type =
            first { $constraint_info{$_} }
        @{$type_preference_for_incrementing};
        croak "Could not handle unique constraint $constraint_name, "
            . "$probe_class does not know how to increment colunms of any "
            . "of the constrained columns' data types."
            unless defined $selected_data_type;
        my ( $selected_unique_col, $max ) =
            @{ @{ $constraint_info{$selected_data_type} }[0] };
        $unique_cols_to_incr{$selected_unique_col} =
            $probe->get_incrementor( $selected_data_type, $max );
    }

    push @handled_columns, keys %unique_cols_to_incr;

    ###HANDLE COLUMNS IN PRIMARY KEY CONSTRAINTS###

    #Determine the dictionary pkey->datatype(pkey) of the pkey columns.
    my %pkey_cols_info = %{ $probe->unique_columns_with_max(1) };

    #Determine the column names in the primary key. This is needed only
    #for determining later on if there is a self-reference.
    my @pkey_column_names;
    my $pkey_col_to_incr;
    my $pkey_col_incrementor;

    if (%pkey_cols_info) {

        #Note: there can only be one primary key, we can therefore
        #select the first element of %pkey_cols_info:
        my $constraint_name = ( keys %pkey_cols_info )[0];
        my %constraint_info = %{ $pkey_cols_info{$constraint_name} };

        for my $data_type ( keys %constraint_info ) {
            for my $col_infos ( $constraint_info{$data_type} ) {
                for my $col_info ( @{$col_infos} ) {
                    push @pkey_column_names, @{$col_info}[0];
                }
            }
        }

        #Determine the pkey column to be incremented and its incrementor
        #similar logic as for unique constraint columns.
        my $selected_data_type =
            first { $constraint_info{$_} }
        @{$type_preference_for_incrementing};
        croak "Could not handle primary key constraint $constraint_name."
            unless defined $selected_data_type;

        my $max;

        ( $pkey_col_to_incr, $max ) =
            @{ @{ $constraint_info{$selected_data_type} }[0] };
        $pkey_col_incrementor =
            $probe->get_incrementor( $selected_data_type, $max );

        push @handled_columns, $pkey_col_to_incr;
    }

    ###HANDLE FOREIGN KEY CONSTRAINTS###

    #We determine lists of foreign keys and tables referenced by these.
    #For each foreign key constraint, the values of a (randomly selected)
    #record from the referenced table will be used for the new record.

    #The referenced table may be the target table itself and in this case
    #the parameter m_maxTreeDepth may come into play, see above.

    #We define dictionaries relating the corresponding columns in the target
    #table to those in the referenced tables.
    my $fkey_tables_ref = $probe->fkey_name_to_fkey_table();

    #skip foreign key handling if there is none

    my ($all_refcol_to_col_dict, $all_refcol_lists,       $fkey_self_ref,
        $parent_pkey_col,        %all_refcol_to_col_dict, %all_refcol_lists,
        $handle_self_ref,        $selfref_tree
    );

    if ( keys %{$fkey_tables_ref} > 0 ) {
        $all_refcol_to_col_dict =
            $probe->fkey_referenced_cols_to_referencing_cols();

        $all_refcol_lists =
            $probe->fkey_referenced_cols( $fkey_tables_ref,
            \@pkey_column_names );

        %all_refcol_to_col_dict = %{$all_refcol_to_col_dict};
        %all_refcol_lists       = %{$all_refcol_lists};

     #If a self-reference is to be handled, define the tree of self-references
     #which will be used to determine the parent records later on.
        if (   $handle_self_ref_wanted
            && defined $pkey_col_to_incr
            && @pkey_column_names == 1 )
        {
            ( $fkey_self_ref, $parent_pkey_col ) = @{
                $probe->get_self_reference( $fkey_tables_ref,
                    $pkey_column_names[0] )
            };
            if ( defined $fkey_self_ref && defined $parent_pkey_col ) {
                $selfref_tree =
                    $probe->selfref_tree( $pkey_col_to_incr,
                    $parent_pkey_col );

                push @handled_columns, $parent_pkey_col;

                $handle_self_ref = 1;
            }
        }

        for ( values %all_refcol_to_col_dict ) {
            push @handled_columns, values $_;
        }
    }

    ###HANDLE COLUMNS WHERE VALUES ARE TAKEN FROM TARGET TABLE ITSELF###

    my @all_cols = @{ $probe->column_names() };

    #Filter out already handled columns.
    my @cols_from_target_table =
        grep {
        my $c = $_;
        !( grep { $_ eq $c } @handled_columns )
        } @all_cols;

    my $cols_from_target_table_list = join ', ', @cols_from_target_table;

    my ( %fkey_random_val_caches, @target_table_cache,
        $pkey_col_to_incr_val );

    $num_records_added = 0;

    #Define the prepared insert statement.
    my $sth_insert =
        $self->dbh->prepare( $probe->insert_statement( \@all_cols ) );

    ###MAIN LOOP: EACH STEP ADDS A NEW RECORD###
    for ( 1 .. $num_records_to_insert ) {
        my %insert = ();

        #Handle pkey column to be increased (if there is one).
        if ( defined $pkey_col_to_incr ) {
            $pkey_col_to_incr_val = $pkey_col_incrementor->();
            $insert{$pkey_col_to_incr} = $pkey_col_to_incr_val;
        }

        #Select the values from tables referenced by foreign keys.
        foreach ( keys %{$fkey_tables_ref} ) {
            my $fkey       = $_;
            my $fkey_table = $fkey_tables_ref->{$fkey};

            #If we have already added enough random records, we select
            #the referenced values from the cache...
            if ( $num_records_added >= $num_random ) {
                %insert = (
                    %insert,
                    %{  @{ $fkey_random_val_caches{$fkey} }
                            [ int rand $num_random ]
                    }
                );
            }

           #...otherwise, get we get the values from randomly selected records
            else {

                #Correspondence between columns in target table and referenced
                #columns:
                my %refcol_to_col_dict = %{ $all_refcol_to_col_dict{$fkey} };

              #List of referenced columns:
              #TODO: check if %all_refcol_lists can be made a simpler data
              #structure: we dereference an array and select the first element
              #only!!
                my $refcol_list = @{ $all_refcol_lists->{$fkey} }[0];

                #If we do not handle a self-reference or the current foreign
                #key is not the one defining it, take the values randomly
                #from the referenced table...
                if ( !$handle_self_ref || $fkey ne $fkey_self_ref ) {
                    my %insert_part =
                        %{ $probe->random_record( $fkey_table, $refcol_list )
                        };

                    #To define our insert we need to replace the column names
                    #from the referenced table by those in the target table.
                    for my $key ( keys %insert_part ) {
                        $insert_part{ $refcol_to_col_dict{$key} } =
                            delete $insert_part{$key};
                    }

                    %insert = ( %insert, %insert_part );

                    #Store the values in a cache.
                    push @{ $fkey_random_val_caches{$fkey} }, \%insert_part;
                }

                #...else handle the self-reference
                else {

                    #Only on first run, determine the current number of roots
                    #and increase the number of random samples if necessary to
                    #get a balanced tree.
                    if ( $num_records_added == 0 ) {

                        $num_roots =
                            $probe->num_roots( $pkey_col_to_incr,
                            $parent_pkey_col );

                        #If we want to have a balanced tree, we want to ensure
                        #the cache of random samples is big enough to
                        #accomodate the minimal number of nodes to complete
                        #the tree, adjusting $num_random if necessary.
                        my ( $r, $i );
                        if ( $min_children > 0 ) {
                            if ( $min_roots > $num_roots ) {
                                $r = $min_roots;
                            }
                            else {
                                $r = $num_roots;
                            }

                            if ( $min_children > 1 ) {
                                $i =
                                    $r
                                    * ( $min_children**$max_tree_depth
                                        - 1 / ( $min_children - 1 ) );
                            }
                            else {
                                $i = $r * $max_tree_depth;
                            }

                            if ( $num_random < $i ) {
                                $num_random = $i;
                            }
                        }
                    }

                    #If necessary, add a root node.
                    if ( $num_roots < $min_roots ) {
                        $selfref_tree->{$pkey_col_to_incr_val} =
                            [$pkey_col_to_incr_val];
                        $insert{$parent_pkey_col} = $pkey_col_to_incr_val;

                        #Store value in cache.
                        push @{ $fkey_random_val_caches{$fkey} },
                            { $parent_pkey_col => $pkey_col_to_incr_val };
                        $num_roots++;
                    }
                    else {
                        my $parent_pkey;

                        #Determine the parent key.
                        ( $selfref_tree, $parent_pkey ) = @{
                            $tree_utils->add_child(
                                $selfref_tree, $pkey_col_to_incr_val,
                                $min_children, $max_tree_depth
                            )
                        };
                        $insert{$parent_pkey_col} = $parent_pkey;

                        #Store value in cache.
                        push @{ $fkey_random_val_caches{$fkey} },
                            { $parent_pkey_col => $parent_pkey };
                    }

                }
            }

        }    #done with foreign key handling

        #Handle unique, non primary key columns to be incremented,
        #these columns get their new value by applying the appropriate
        #incrementor.
        for ( keys %unique_cols_to_incr ) {
            $insert{$_} = $unique_cols_to_incr{$_}->();
        }

        #Handle columns selected from target table itself if there
        #are any such columns left to be processed.
        if ( @cols_from_target_table > 0 ) {
            if ( $num_records_added >= $num_random ) {

                #Select values randomly from the cache.
                %insert = (
                    %insert, %{ $target_table_cache[ int rand $num_random ] }
                );
            }
            else {

                #Select values randomly from the target table.
                my $values =
                    $probe->random_record( $self->table,
                    $cols_from_target_table_list );
                %insert = ( %insert, %{$values} );

                #Store values in cache.
                push @target_table_cache, $values;
            }
        }

        #Execute the insert.
        my @val_list = map { $insert{$_} } @all_cols;
        $sth_insert->execute(@val_list);

        $num_records_added++;
    }

    #Commit all inserts. From DBI doc: If AutoCommit is on, then calling
    #commit will issue a "commit ineffective with AutoCommit" warning.
    $self->dbh->commit()
        unless $self->dbh->{AutoCommit}
        or croak "Could not commit the inserts:\n" . $self->dbh->errstr;

    return $num_records_added;
}

1;    # End of DBIx::Table::TestDataGenerator

__END__

=pod

=head1 NAME

DBIx::Table::TestDataGenerator - Automatic test data creation, cross DBMS

=head1 VERSION

Version 0.0.1

=head1 SYNOPSIS

    use DBIx::Table::TestDataGenerator;

    my $generator = DBIx::Table::TestDataGenerator->new(
            dbh                => $dbi_database_handle,
            schema             => $schema_name,
            table              => $target_table_name,
    );

    #simple usage:
    $generator->create_testdata(
        target_size    => $target_size,
        num_random     => $num_random,
        seed           => $seed,
    );

    #extended usage handling a self-reference of the target table:
    $generator->create_testdata(
            target_size    => $target_size,
            num_random     => $num_random,
            seed           => $seed,
            max_tree_depth => $max_tree_depth,
            min_children   => $min_children,
            min_roots      => $min_roots,
    );


    #instantiation using a custom DBMS handling class
    my $generator = DBIx::Table::TestDataGenerator->new(
            dbh                => $dbi_database_handle,
            schema             => $schema_name,
            table              => $target_table_name,
            custom_probe_class => $custom_probe_class_name,
    );

=head1 DESCRIPTION

There is often the need to create test data in database tables, e.g. to test database client performance. The existence of constraints on a table makes it non-trivial to come up with a way to add records to it.

The current module inspects the tables' constraints and adds a desired number of records. The values of the fields either come from the table itself (possibly incremented to satisfy uniqueness constraints) or from tables referenced by foreign key constraints. The choice of the copied values is random for a number of runs the user can choose, afterwards the values are chosen randomly from a cache, reducing database traffic for performance reasons. The user can define seeds for the randomization to be able to reproduce a test run. One nice thing about this way to construct new records is that at least at first sight, the added data looks like real data, at least as real as the data initially present in the table was.

A main goal of the module is to reduce configuration to the absolute minimum by automatically determining information about the target table, in particular its constraints. Another goal is to support as many DBMSs as possible. Currently Oracle, PostgreSQL and SQLite are supported, further DBMSs are in the work and one can add further databases or change the default behaviour by writing a class satisfying the role defined in DBIx::Table::TestDataGenerator::TableProbe.pm. NOTE: A major refactoring is on its way, see section FURTHER DEVELOPMENT.

In the synopsis, an extended usage has been mentioned. This refers to the common case of having a self-reference on a table, i.e. a one-column wide foreign key of a table to itself where the referenced column constitutes the primary key. Such a parent-child relationship defines a rootless tree and when generating test data it may be useful to have some control over the growth of this tree. One such case is when the parent-child relation represents a navigation tree and a client application processes this structure. In this case, one would like to have a meaningful, balanced tree structure since this corresponds to real-world examples. To control tree creation the parameters max_tree_depth, min_children and min_roots are provided. Note that the nodes are being added in a depth-first manner.

=head1 SUBROUTINES/METHODS

=head2 new

Arguments:

=over 4

=item * dbh: required DBI database handle

=item * schema: optional database schema name

=item * table: required name of the target table

=item * custom_probe_class: optional custom probe class name

=back

Return value:

a new TestDataGenerator object

Creates a new TestDataGenerator object. If the DBMS in question does not support the concept of a schema, the corresponding argument may be omitted. If a DBMS currently not supported by DBI::Table::TestDataGenerator is to be supported, or the behaviour of the current TableProbe class responsible for handling the DBMS must be changed, one may provide the optional custom_probe_class parameter. custom_probe_class being the name of a custom class impersonating the TableProbe role.

=head2 dbh

Accessor for the DBI database handle.

=head2 schema

Accessor for the database schema name.

=head2 table

Accessor for the name of the target table.

=head2 custom_probe_class

Accessor for the name of a custom class impersonating the TableProbe role.

=head2 create_testdata

This is the main method, it creates and adds new records to the target table. In case one of the arguments max_tree_depth, min_children or min_roots has been provided, the other two must be provided as well.

Arguments:

=over 4

=item * target_size

The target number of rows to be reached.

=item * num_random

The first $num_random number of records use fresh random choices for their values taken from tables referenced by foreign key relations or the target table itself. These values are stored in a cache and re-used for the remaining (target_size - $num_random) records. Note that even for the remaining records there is some randomness since the combination of cached values coming from columns involved in different constraints is random.

=item * seed

This value must be an integer. In case it has been provided, the random selections done by the Perl code as well as those done by the database (where supported, e.g. not for SQLite) are seeded by this value resp. a value based on this value, e.g. PostgreSQL accepting only floating numbers between 0 and 1. This allows for reproducible test runs.

=item * max_tree_depth

In case of a self-reference, the maximum depth at which new records will be inserted. The minimum value for this parameter is 2.

=item * min_children

In case of a self-reference, the minimum number of children each handled parent node will get. A possible exception is the last handled parent node if the execution stops before $min_children child nodes have been added to it.

=item * min_roots

In case of a self-reference, the minimum number of root elements existing after completion of the call to create_testdata. A record is considered to be a root element if the corresponding parent id is null or equal to the child id.

=back

Returns:

Nothing, only called for the side-effect of adding new records to the target table. (This may change, see the section FURTHER DEVELOPMENT.)

=head1 INSTALLATION AND CONFIGURATION

To install this module, run the following commands:

	perl Build.PL
	./Build
	./Build test
	./Build install

When installing from CPAN, the install tests look for the environment variables TDG_DSN (connection string), TDG_USER (user), TDG_PWD (password) and TDG_SCHEMA (schema) which may be used to test the installation against an existing database. If TDG_DSN is found, the install will try to use this connection string and the tests will fail if no valid database connection can be established. If TDG_DSN is not found, the installation creates an in-memory SQLite database provided for free by the DBD::SQLite module and tests against this database.

=head1 DATABASE VERSIONS TESTED AGAINST

=over 4

=item * SQLite 3.7.14.1

=item * Oracle 11g XE

=item * PostgreSQL 9.2.1

=back

=head1 LIMITATIONS

=over 4

=item * Currently, the module executes the inserts in one big transaction if the database handle has not set AutoCommit to true, but this will change, see the section FURTHER DEVELOPMENT.

=item * Only uniqueness and foreign key constraints are taken into account. Constraints such as check constraints, which are very diverse and database specific, are not handled (and most probably will not be).

=item * Uniqueness constraints involving only columns which the DBMS specific TableProbe role handler does not know how to increment cannot be handled. Typically, all string and numeric data types are supported and the set of supported data types is defined by the list provided by the TableProbe role method get_type_preference_for_incrementing(). I am thinking about allowing date incrementation, too, it would be necessary then to at least add a configuration parameter defining what time incrementation step to use.

=item * When calling create_testdata, max_tree_depth = 1 should be allowed, too, meaning that all new records will be root records.

=item * Added records that are root node with respect to the self-reference always have the parent id equal to their pkey. It may be that in the case in question the convention is such that root nodes are identified by having the parent id set to NULL.

=back

=head1 FURTHER DEVELOPMENT

=over 4

=item * A major refactoring planned to be released with version 0.003 is in the works where I want to remove database specific handling with the help of DBIx::Class. Even if some DBMS specifics are left, this will help to support a broad range of DBMSs and the matureness of DBIx::Class will certainly help to keep the number of bugs low.

=item * The current version handles uniqueness constraints by picking out a column involved in the constraint and incrementing it appropriately. While one may do something different in a custom TableProbe class than incrementing and even if the values are being incremented, the calculation of the increment may be different, one is constrained to handling the single selected column.

=item * Support for transactions and specifying transaction sizes will be added.

=item * It will be possible to get the SQL source of all generated inserts without having them executed on the database.

=back

=head1 ACKNOWLEDGEMENTS

=over 4

=item * Version 0.001:

A big thank you to all perl coders on the dbi-dev, DBIx-Class and perl-modules mailing lists and on PerlMonks who have patiently answered my questions and offered solutions, advice and encouragement, the Perl community is really outstanding.

Special thanks go to Tim Bunce (module name / advice on keeping the module extensible), Jonathan Leffler (module naming discussion / relation to existing modules / multiple suggestions for features), brian d foy (module naming discussion / mailing lists / encouragement) and the following Perl monks (see the threads for user jds17 for details): chromatic, erix,  technojosh, kejohm, Khen1950fx, salva, tobyink (3 of 4 discussion threads!), Your Mother.

=item * Version 0.002:

Martin J. Evans was the first developer giving me feedback and nice bug reports on Version 0.001, thanks a lot!

=back

=head1 AUTHOR

Jose Diaz Seng, C<< <josediazseng at gmx.de> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dbix-table-testdatagenerator at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-Table-TestDataGenerator>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DBIx::Table::TestDataGenerator

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-Table-TestDataGenerator>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DBIx-Table-TestDataGenerator>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DBIx-Table-TestDataGenerator>

=item * Search CPAN

L<http://search.cpan.org/dist/DBIx-Table-TestDataGenerator/>

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Jose Diaz Seng.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

