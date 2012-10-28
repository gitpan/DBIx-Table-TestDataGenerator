package DBIx::Table::TestDataGenerator::TableProbe::Oracle;
use Moo;
use Moo::Role;

use strict;
use warnings;

use DBIx::Table::TestDataGenerator;
use DBIx::Admin::TableInfo;

use Carp;

with 'DBIx::Table::TestDataGenerator::TableProbe';

use Readonly;
Readonly my $COMMA         => q{,};
Readonly my $QUESTION_MARK => q{?};

sub column_names {
    my ($self) = @_;
    my $info = DBIx::Admin::TableInfo->new(
        dbh    => $self->dbh,
        schema => uc $self->schema,
        table  => uc $self->table
    )->info();
    return [ keys ${$info}{ uc $self->table }{'columns'} ];
}

sub num_roots {
    my ( $self, $pkey_col, $parent_pkey_col ) = @_;
    my $table  = $self->table;
    my $schema = $self->schema;
    my $sql    = <<"END_SQL";
SELECT COUNT($parent_pkey_col)
FROM $schema.$table
WHERE $pkey_col = $parent_pkey_col OR $parent_pkey_col IS NULL
END_SQL
    my $sth = $self->dbh->prepare($sql);
    $sth->execute();
    return ( $sth->fetchrow_array() )[0];
}

sub seed {
    my ( $self, $random_seed ) = @_;

    #DBMS_RANDOM.SEED expects seed of type BINARY_INTEGER OR VARCHAR2
    $self->dbh->do("begin\nDBMS_RANDOM.SEED($random_seed); end; ");
    return;
}

sub random_record {
    my ( $self, $table, $colname_list ) = @_;
    my $schema = $self->schema;
    my $sql    = <<"END_SQL";
SELECT $colname_list
FROM (
  SELECT * FROM $schema.$table ORDER BY DBMS_RANDOM.VALUE
)
WHERE ROWNUM = 1
END_SQL
    return $self->dbh->selectrow_hashref($sql);
}

sub get_incrementor {
    my ( $self, $type, $max ) = @_;
    if ( $type =~ /CHAR/ ) {
        my $i      = 0;
        my $suffix = 'A' x $max;
        return sub {
            return $suffix . $i++;
            }
    }
    if ( $type =~ /NUMBER/ ) {
        return sub { return ++$max };
    }
    if ( $type =~ /DATE/ ) {
        croak 'cannot handle unique constraints having only date columns';
    }
    croak
        "I do not know how to increment unique constraint column of type $type";
}

sub get_type_preference_for_incrementing {
    my @types = qw(INTEGER INT SMALLINT NUMBER NUMERIC FLOAT DEC DECIMAL
        REAL DOUBLEPRECISION CHAR NCHAR NVARCHAR2 VARCHAR2 LONG);
    return \@types;
}

sub unique_columns_with_max {
    my ( $self, $get_pkey_columns ) = @_;

    my $sql;
    my $table_name = uc $self->table;
    my $schema     = uc $self->schema;
    if ($get_pkey_columns) {

        #Note: we need to exclude columns which are also part of
        #other unique constraints
        $sql = <<"END_SQL";
SELECT c.constraint_name, cc.column_name, tc.data_type
FROM all_indexes i, all_constraints c,
    all_cons_columns cc, user_tab_columns tc
WHERE  i.index_name = c.constraint_name
       AND c.constraint_type = 'P'
       AND c.owner = '$schema'
       AND i.uniqueness = 'UNIQUE'
       AND i.table_name = '$table_name'
       AND i.table_owner = '$schema'
       AND cc.constraint_name = C.constraint_name
       AND cc.owner = '$schema'
       AND tc.table_name = i.table_name
       AND tc.column_name = cc.column_name
       AND cc.column_name NOT IN (
          SELECT column_name
          FROM (
            SELECT column_name, constraint_name
            FROM all_cons_columns
            WHERE table_name = '$table_name'
            ) cc1
          JOIN (
            SELECT constraint_name
            FROM all_constraints
            WHERE OWNER = '$schema' AND constraint_type IN ('R', 'U')
            ) c1
            ON cc1.constraint_name = c1.constraint_name
       )
END_SQL

    }
    else {
        $sql = <<"END_SQL";
SELECT c.constraint_name, cc.column_name, tc.data_type
FROM all_indexes i, all_constraints c,
    all_cons_columns cc, user_tab_columns tc
WHERE  i.index_name = c.constraint_name
       AND c.constraint_type <> 'P'
       AND c.owner = '$schema'
       AND i.uniqueness = 'UNIQUE'
       AND i.table_name = '$table_name'
       AND i.table_owner = '$schema'
       AND cc.constraint_name = C.constraint_name
       AND cc.owner = '$schema'
       AND tc.table_name = i.table_name
       AND tc.column_name = cc.column_name
END_SQL

    }
    my %uniq_col_info;
    my $sth = $self->dbh->prepare($sql);
    $sth->execute();

    while ( my @row = $sth->fetchrow_array() ) {
        my ( $constr, $col, $data_type ) = @row;
        $uniq_col_info{$constr} ||= {};
        $uniq_col_info{$constr}->{$data_type} ||= [];

        my %max_expr = (
            'NUMBER'   => "MAX($col)",
            'DATE'     => "MAX($col)",
            'VARCHAR2' => "MAX(LENGTH($col))",
            'CHAR'     => "MAX(LENGTH($col))",
        );

        my $max_sql = <<"END_SQL";
SELECT $max_expr{$data_type}
FROM $schema.$table_name
END_SQL

        my $max_sth = $self->dbh->prepare($max_sql);
        $max_sth->execute();
        my $max_val = ( $max_sth->fetchrow_array() )[0];
        push @{ $uniq_col_info{$constr}->{$data_type} }, [ $col, $max_val ];
    }
    return \%uniq_col_info;
}

sub fkey_name_to_fkey_table {
    my ($self)     = @_;
    my $table_name = uc $self->table;
    my $schema     = uc $self->schema;
    my $sql        = <<"END_SQL";
SELECT DISTINCT ac0.constraint_name
FROM sys.all_cons_columns c0, sys.all_constraints ac0
WHERE  c0.table_name = '$table_name'
       AND c0.owner = '$schema'
       AND c0.constraint_name = ac0.constraint_name
       AND ac0.constraint_type = 'R'
       AND ac0.owner = '$schema'
       AND NOT EXISTS
          ( SELECT COUNT (c.column_name), c.constraint_name
            FROM sys.all_cons_columns c, sys.all_constraints ac
            WHERE  c.table_name = '$table_name'
                   AND c.owner = '$schema'
                   AND C.constraint_name = AC.constraint_name
                   AND ac.constraint_type = 'R'
                   AND ac.owner = '$schema'
                   AND c0.column_name IN (SELECT column_name
                                          FROM sys.all_cons_columns
                                          WHERE constraint_name = ac.constraint_name)
HAVING COUNT (c.column_name) > (SELECT COUNT (column_name)
                                FROM sys.all_cons_columns
                                WHERE constraint_name = ac0.constraint_name)
GROUP BY c.constraint_name)
END_SQL

    my $sth = $self->dbh->prepare($sql);
    $sth->execute();

    my %fkey_tables;

    while ( my @row = $sth->fetchrow_array ) {
        my $fkey_name = $row[0];
        $sql = <<"END_SQL";
SELECT a.table_name
FROM   user_constraints a
     JOIN
       user_constraints b
     ON  a.constraint_name = B.R_constraint_name
         AND B.constraint_name = UPPER('$fkey_name')
         AND a.owner = '$schema'
         AND b.owner = '$schema'
END_SQL

        $sth = $self->dbh->prepare($sql);
        $sth->execute();
        $fkey_tables{$fkey_name} = ( $sth->fetchrow_array )[0];
    }

    return \%fkey_tables;
}

sub fkey_referenced_cols_to_referencing_cols {
    my ($self) = @_;
    my $schema = uc $self->schema;
    my $sql    = <<"END_SQL";
SELECT CC2.COLUMN_NAME AS cons_col, CC1.COLUMN_NAME AS ref_col
FROM sys.all_cons_columns cc1, sys.user_constraints uc, sys.all_cons_columns cc2
WHERE  CC1.constraint_name = UC.constraint_name       
       AND UC.R_constraint_name = CC2.constraint_name
       AND cc1.constraint_name = ?
       AND CC1.POSITION = cc2.position
       AND CC1.OWNER = '$schema'
       AND cc2.owner = '$schema'
       AND uc.owner = '$schema'
ORDER BY cc1.position
END_SQL
    my $sth = $self->dbh->prepare($sql);

    my %all_refcol_to_col_dict;

    my @fkey_names = keys %{ $self->fkey_name_to_fkey_table() };

    foreach (@fkey_names) {
        my $fkey = $_;
        $sth->execute($fkey);
        my %refcol_to_col_dict;
        while ( my @row = $sth->fetchrow_array() ) {
            $refcol_to_col_dict{ $row[0] } = $row[1];
        }

        $all_refcol_to_col_dict{$fkey} = \%refcol_to_col_dict;
    }

    return \%all_refcol_to_col_dict;
}

sub fkey_referenced_cols {
    my ( $self, $fkey_tables ) = @_;
    my $schema = uc $self->schema;
    my $sql    = <<"END_SQL";
SELECT CC2.COLUMN_NAME AS cons_col
FROM sys.all_cons_columns cc1, sys.user_constraints uc, sys.all_cons_columns cc2
WHERE  CC1.constraint_name = UC.constraint_name
       AND UC.R_constraint_name = CC2.constraint_name
       AND cc1.constraint_name = ?
       AND CC1.POSITION = cc2.position
       AND CC1.OWNER = '$schema'
       AND cc2.owner = '$schema'
       AND uc.owner = '$schema'
ORDER BY cc1.position
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
    my $schema = uc $self->schema;
    my $sql    = <<"END_SQL";
SELECT CC2.COLUMN_NAME AS cons_col, CC1.COLUMN_NAME AS ref_col
FROM sys.all_cons_columns cc1, sys.user_constraints uc, sys.all_cons_columns cc2
WHERE  CC1.constraint_name = UC.constraint_name
       AND UC.R_constraint_name = CC2.constraint_name
       AND cc1.constraint_name = ?
       AND CC1.POSITION = cc2.position
       AND CC1.OWNER = '$schema'
       AND cc2.owner = '$schema'
       AND uc.owner = '$schema'
ORDER BY cc1.position
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

        if (   $ref_table eq uc( $self->table )
            && @ref_col_list == 1
            && $pkey_col_name eq $ref_col_list[0] )
        {
            @self_ref_info = ( $fkey, [ values %refcol_to_col_dict ]->[0] );
            last;
        }
    }

    return \@self_ref_info;
}

sub selfref_tree {
    my ( $self, $key_col, $parent_refkey_col ) = @_;
    my $table_name = $self->table;
    my $sql        = <<"END_SQL";
SELECT t.$key_col, t1.$key_col
FROM $table_name t, $table_name t1
WHERE t.$parent_refkey_col = t1.$key_col(+)

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

1;    # End of DBIx::Table::TestDataGenerator::TableProbe::Oracle

__END__

=pod

=head1 NAME

DBIx::Table::TestDataGenerator::TableProbe::Oracle - Oracle (meta)data provider

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

