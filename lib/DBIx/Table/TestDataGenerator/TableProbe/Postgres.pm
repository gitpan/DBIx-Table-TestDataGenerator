package DBIx::Table::TestDataGenerator::TableProbe::Postgres;
use Moo;
use Moo::Role;

use strict;
use warnings;

use Carp;

use List::MoreUtils qw ( any );

use DBIx::Table::TestDataGenerator;

use Readonly;
Readonly my $COMMA         => q{,};
Readonly my $QUESTION_MARK => q{?};

with 'DBIx::Table::TestDataGenerator::TableProbe';

sub column_names {
    my ($self) = @_;
    my $sql = <<"END_SQL";
SELECT column_name
FROM INFORMATION_SCHEMA.columns
WHERE table_catalog = '${\$self->database}'
      AND table_schema = '${\$self->schema}'
      AND table_name = '${\$self->table}'
END_SQL
    my $sth = $self->dbh->prepare($sql);
    $sth->execute();
    my @columns;
    while ( my $col = $sth->fetchrow_array() ) {
        push @columns, $col;
    }
    return \@columns;
}

sub num_roots {
    my ( $self, $pkey_col, $parent_pkey_col ) = @_;
    my $table  = $self->table;
    my $schema = $self->schema;

    #note: PostgreSQL ignores NULL when counting values!
    #Therefore we use Coalesce to first replace NULL values by 0
    my $sql = <<"END_SQL";
SELECT COUNT(COALESCE($parent_pkey_col, 0))
FROM $schema.$table
WHERE $pkey_col = $parent_pkey_col OR $parent_pkey_col IS NULL
END_SQL
    my $sth = $self->dbh->prepare($sql);
    $sth->execute();
    return ( $sth->fetchrow_array() )[0];
}

sub seed {
    my ( $self, $random_seed ) = @_;

    #$random_seed must be a floating-point number between 0 and 1,
    #we know an integer will be passed in, so we use its digits to
    #define a suitable floating-point number
    $random_seed = '0.' . $random_seed;
    $self->dbh->do("SELECT setseed($random_seed)");
    return;
}

sub random_record {
    my ( $self, $table, $colname_list ) = @_;
    my $schema = $self->schema;
    my $sql    = <<"END_SQL";
SELECT $colname_list
FROM (
  SELECT *
  FROM $schema.$table
  OFFSET RANDOM()*(SELECT COUNT(*)-1 FROM $table)
  LIMIT 1) t
END_SQL
    return $self->dbh->selectrow_hashref($sql);
}

{
    my @num_types = qw(smallint integer bigint decimal numeric
        real double precision serial bigserial);
    my @chr_types = ( 'character varying', qw( character char varchar text) );
    my @date_types = qw(date);

    sub get_incrementor {
        my ( $self, $type, $max ) = @_;
        if ( any { $type eq $_ } @num_types )
            {
                return sub { return ++$max };
        }
        if ( any { $type eq $_ } @chr_types )
            {
                my $i      = 0;
                my $suffix = 'A' x $max;
                return sub {
                    return $suffix . $i++;
                    }
        }
        if ( any { $type eq $_ } @date_types )
            {
                croak
                    'cannot handle unique constraints having only date columns';
        }
        croak
            "I do not know how to increment unique constraint column of type $type";
    }
}

sub get_type_preference_for_incrementing {
        return [ 'integer', 'numeric', 'real',
            'double precision', 'bigint',            'smallint',
            'character',        'character varying', 'text' ];
}

sub unique_columns_with_max {
        my ( $self, $get_pkey_columns ) = @_;

        my $sql;
        my $database   = $self->database;
        my $schema     = $self->schema;
        my $table_name = $self->table;
        my $key_type   = $get_pkey_columns ? 'PRIMARY KEY' : 'UNIQUE';
        $sql = <<"END_SQL";
SELECT cu.constraint_name, cu.column_name, c.data_type
FROM INFORMATION_SCHEMA.table_constraints tc,
     INFORMATION_SCHEMA.key_column_usage cu,
     INFORMATION_SCHEMA.columns c
WHERE tc.constraint_name = cu.constraint_name AND cu.constraint_schema = '$schema'
  AND tc.table_schema = '$schema' AND tc.table_name = cu.table_name 
  AND tc.table_name = '$table_name' AND c.table_name = '$table_name' 
  AND c.table_schema = '$schema' AND c.column_name = cu.column_name
  AND EXISTS ( SELECT tc.*
               FROM INFORMATION_SCHEMA.table_constraints tc1
               WHERE tc1.constraint_catalog = '$database'
                     AND tc1.table_name = '$table_name'
                     AND tc1.table_schema = '$schema'
                     AND tc1.constraint_type = '$key_type'
                     AND tc1.constraint_name = cu.constraint_name)
END_SQL

        my %uniq_col_info;
        my $sth = $self->dbh->prepare($sql);
        $sth->execute();

        while ( my @row = $sth->fetchrow_array() ) {
            my ( $constr, $col, $data_type ) = @row;
            $uniq_col_info{$constr} ||= {};
            $uniq_col_info{$constr}->{$data_type} ||= [];

            my %max_expr = (
                'smallint'          => "MAX($col)",
                'integer'           => "MAX($col)",
                'bigint'            => "MAX($col)",
                'decimal'           => "MAX($col)",
                'numeric'           => "MAX($col)",
                'real'              => "MAX($col)",
                'double precision'  => "MAX($col)",
                'serial'            => "MAX($col)",
                'bigserial'         => "MAX($col)",
                'character varying' => "MAX(LENGTH($col))",
                'character'         => "MAX(LENGTH($col))",
                'char'              => "MAX(LENGTH($col))",
                'varchar'           => "MAX(LENGTH($col))",
                'text'              => "MAX(LENGTH($col))",
                'date'              => "MAX($col)",
                );

            my $max_sql = <<"END_SQL";
SELECT $max_expr{$data_type}
FROM $table_name
END_SQL

            my $max_sth = $self->dbh->prepare($max_sql);
            $max_sth->execute();
            my $max_val = ( $max_sth->fetchrow_array() )[0];
            push @{ $uniq_col_info{$constr}->{$data_type} }, [ $col,
                $max_val ];
        }
        return \%uniq_col_info;
}

sub fkey_name_to_fkey_table {
        my ($self)     = @_;
        my $database   = $self->database;
        my $schema     = $self->schema;
        my $table_name = $self->table;
        my $sql        = <<"END_SQL";
SELECT c.constraint_name, u.table_name
FROM INFORMATION_SCHEMA.table_constraints c,
     INFORMATION_SCHEMA.constraint_table_usage u
WHERE c.constraint_catalog = '$database'
      AND c.table_schema = '$schema'
      AND c.table_name = '$table_name'
      AND c.constraint_type = 'FOREIGN KEY'
      AND c.table_schema = u.table_schema
      AND c.constraint_name = u.constraint_name;
END_SQL

        my $sth = $self->dbh->prepare($sql);
        $sth->execute();

        my %fkey_tables;

        while ( my @row = $sth->fetchrow_array ) {
            $fkey_tables{ $row[0] } = $row[1];
        }
        return \%fkey_tables;
}

sub fkey_referenced_cols_to_referencing_cols {
        my ($self)     = @_;
        my $schema     = $self->schema;
        my $table_name = $self->table;
        my $sql        = <<"END_SQL";
SELECT
    pc.conname AS fkey,
    pap.attname AS ref_col,
    pac.attname AS cons_col
FROM
    (SELECT connamespace,conname, unnest(conkey) AS "conkey",
            unnest(confkey) AS "confkey" , conrelid, confrelid,
            contype
     FROM pg_constraint) pc
    JOIN pg_namespace pn ON pc.connamespace = pn.oid
    JOIN pg_class pclsc ON pc.conrelid = pclsc.oid
    JOIN pg_class pclsp ON pc.confrelid = pclsp.oid
    JOIN pg_attribute pac ON pc.conkey = pac.attnum
                             AND pac.attrelid = pclsc.oid
    JOIN pg_attribute pap ON pc.confkey = pap.attnum
                             AND pap.attrelid = pclsp.oid
    WHERE nspname = '$schema' AND pclsc.relname = '$table_name'
ORDER BY pclsc.relname;
END_SQL

        my $sth = $self->dbh->prepare($sql);

        my %all_refcol_to_col_dict;

        $sth->execute();
        my %refcol_to_col_dict;
        while ( my ( $fkey, $ref_col, $cons_col ) = $sth->fetchrow_array() ) {
            if ( !defined $all_refcol_to_col_dict{$fkey} ) {
                $all_refcol_to_col_dict{$fkey} = {};
            }
            ${ $all_refcol_to_col_dict{$fkey} }{$ref_col} = $cons_col;
        }

        return \%all_refcol_to_col_dict;
}

sub fkey_referenced_cols {
        my ( $self, $fkey_tables ) = @_;
        my $schema = $self->schema;
        my $sql    = <<"END_SQL";
SELECT column_name
FROM INFORMATION_SCHEMA.constraint_column_usage
WHERE constraint_name = ?
      AND table_schema = '$schema';
END_SQL

        my $sth = $self->dbh->prepare($sql);

        my %all_refcol_lists;

        foreach ( keys %{$fkey_tables} ) {
            my $fkey = $_;

            $sth->execute($fkey);
            my @ref_col_list;
            while ( my @row = $sth->fetchrow_array() ) {
                push @ref_col_list, $row[0];
            }

            my @ref_cols = join ', ', @ref_col_list;
            $all_refcol_lists{$fkey} = \@ref_cols;
        }

        return \%all_refcol_lists;
}

sub get_self_reference {
        my ( $self, $fkey_tables, $pkey_col_name ) = @_;
        my $database   = $self->database;
        my $schema     = $self->schema;
        my $table_name = $self->table;

       #note: in PostgreSQL, foreign key names are unique only within a table,
       #we therefore need to take the target table name into account
        my $sql = <<"END_SQL";
SELECT
    pap.attname as cons_col,
    pac.attname as ref_col
FROM
    (
    SELECT
         connamespace,conname, unnest(conkey) as "conkey", unnest(confkey)
          as "confkey" , conrelid, confrelid, contype
     FROM
        pg_constraint
    ) pc
    JOIN pg_namespace pn ON pc.connamespace = pn.oid
    JOIN pg_class pclsc ON pc.conrelid = pclsc.oid
    JOIN pg_class pclsp ON pc.confrelid = pclsp.oid
    JOIN pg_attribute pac ON pc.conkey = pac.attnum  AND pac.attrelid = pclsc.oid
    JOIN pg_attribute pap ON pc.confkey = pap.attnum AND pap.attrelid = pclsp.oid
WHERE pc.conname = ?
ORDER BY pclsc.relname
END_SQL

        my $sth = $self->dbh->prepare($sql);

        my @self_ref_info;

        foreach ( keys %{$fkey_tables} ) {
            my $fkey      = $_;
            my $ref_table = $fkey_tables->{$fkey};

            $sth->execute($fkey);
            my %refcol_to_col_dict;
            my @ref_col_list;
            while ( my @row = $sth->fetchrow_array() ) {
                $refcol_to_col_dict{ $row[0] } = $row[1];
                push @ref_col_list, $row[0];
            }

            if ( uc $ref_table eq uc( $self->table )
                && @ref_col_list == 1
                && $pkey_col_name eq $ref_col_list[0] )
            {
                @self_ref_info =
                    ( $fkey, [ values %refcol_to_col_dict ]->[0] );
                last;
            }
        }

        return \@self_ref_info;
}

sub selfref_tree {
        my ( $self, $key_col, $parent_refkey_col ) = @_;
        my $table_name = $self->table;
        my $schema     = $self->schema;
        my $sql        = <<"END_SQL";
SELECT t.$key_col, t1.$key_col
FROM $schema.$table_name t LEFT OUTER JOIN $table_name t1
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

1;    # End of DBIx::Table::TestDataGenerator::TableProbe::Postgres

__END__

=pod

=head1 NAME

DBIx::Table::TestDataGenerator::TableProbe::Postgres - PostgreSQL (meta)data provider

=head1 SUBROUTINES/METHODS

For TableProbe role methods, see the documentation of L<TableProbe|DBIx::Table::TestDataGenerator::TableProbe>.

=head1 AUTHOR

Jos\x{00E9} Diaz Seng, C<< <josediazseng at gmx.de> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Jos\x{00E9} Diaz Seng.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

