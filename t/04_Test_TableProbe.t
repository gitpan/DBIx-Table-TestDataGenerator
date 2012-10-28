#!perl

use 5.006;
use strict;
use warnings;

use version; our $VERSION = qv('0.0.1_1');

use Carp;

use Test::More;

use DBI;
use Class::Load qw (load_class);
use DBIx::Table::TestDataGenerator::DBDriverUtils;

plan tests => 10;

my $db_driver_utils = DBIx::Table::TestDataGenerator::DBDriverUtils->new();

my ( $dsn, $dbuser, $dbpwd ) =
    ( $ENV{'TDG_DSN'}, $ENV{'TDG_USER'}, $ENV{'TDG_PWD'} );

my $dbh =
    defined $dsn
    ? DBI->connect( $dsn, $dbuser, $dbpwd )
    : $db_driver_utils->get_in_memory_dbh();

#note: Oracle converts values of columns in system tables to uppercase,
#e.g. all_cons_columns.table_name, user_tab_columns.table_name,
#all_constraints.owner. To avoid scattering calls to UPPER in PL/SQL code
#we make the relevant input uppercase here:
my $table = 'test_helperqueries';

my $probe_class_short = $db_driver_utils->db_driver_name($dbh);
my $probe_class =
    "DBIx::Table::TestDataGenerator::TableProbe::$probe_class_short";

load_class($probe_class);

#create test table
sub create_test_table {
    my $sql = <<"END_SQL";
CREATE TABLE $table (
  id  INTEGER NOT NULL,
  refid INTEGER,
  ud CHAR(3),
  dt DATE,
  CONSTRAINT pkey PRIMARY KEY (id),
  CONSTRAINT ud_dt_unique UNIQUE(ud,dt),
  CONSTRAINT fk_id FOREIGN KEY (refid)
    REFERENCES $table (id)
)
END_SQL

    $dbh->do($sql);
    return;
}

sub create_test_table_sqlite {
    my $sql = <<"END_SQL";
CREATE TABLE $table (
  id  INTEGER NOT NULL,
  refid INTEGER,
  ud TEXT,
  dt TEXT,
  PRIMARY KEY (id),
  UNIQUE(ud,dt),
  CONSTRAINT fk_id FOREIGN KEY (refid)
    REFERENCES $table (id)
)
END_SQL

    $dbh->do($sql);
    return;
}

sub fill_test_table {
    $dbh->do(
        "INSERT INTO $table (id, refid, ud, dt) VALUES (1,1,'A','12.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $table (id, refid, ud, dt) VALUES (2,1,'B1','12.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $table (id, refid, ud, dt) VALUES (3,1,'BB','13.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $table (id, refid, ud, dt) VALUES (4,4,'CCC','15.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $table (id, refid, ud, dt) VALUES (5,4,'X','11.04.2011')"
    );
    return;
}

if ( $probe_class_short eq 'SQLite' ) {
    create_test_table_sqlite();
}
else {
    create_test_table();
}

my $probe = $probe_class->new(
    dbh      => $dbh,
    database => $db_driver_utils->get_database($dbh),
    schema   => $probe_class_short eq 'Oracle' ? $dbuser : 'public',
    table    => $table
);

my @col_names = sort @{ $probe->column_names() };

is( @col_names, 4, "$table has four columns" );

if ( $probe_class_short eq 'Oracle' ) {
    is_deeply(
        \@col_names,
        [ 'DT', 'ID', 'REFID', 'UD' ],
        'correct column names'
    );
}
else {
    is_deeply(
        \@col_names,
        [ 'dt', 'id', 'refid', 'ud' ],
        'correct column names'
    );
}

is( $probe->num_records(), 0, 'just created table, 0 records in there' );

fill_test_table();

is( $probe->num_records(), 5, 'fill_test_table inserted 5 records' );

my %h = %{ $probe->unique_columns_with_max(0) };

if ( $probe_class_short eq 'Oracle' ) {
    is_deeply(
        \%h,
        {   'UD_DT_UNIQUE' => {
                'CHAR' => [ [ 'UD', 3 ] ],
                'DATE' => [ [ 'DT', '15.04.11' ] ]
            }
        },
        'unique constraint columns information correctly determined'
    );

    my %pkey_info = %{ $probe->unique_columns_with_max(1) };
    ok( eq_hash( \%pkey_info, { 'PKEY' => { 'NUMBER' => [ [ 'ID', 5 ] ] } } ),
        'pkey column information correctly determined'
    );

    my $fkey_tables_ref = $probe->fkey_name_to_fkey_table();
    ok( eq_hash( $fkey_tables_ref, { 'FK_ID' => uc $table } ),
        'correct foreign key determined' );

    my $all_refcol_to_col_dict =
        $probe->fkey_referenced_cols_to_referencing_cols( $fkey_tables_ref,
        ['ID'] );

    my $all_refcol_lists =
        $probe->fkey_referenced_cols( $fkey_tables_ref, ['ID'] );

    my $fkey_self_ref =
        @{ $probe->get_self_reference( $fkey_tables_ref, 'ID' ) }[1];

    ok( eq_hash(
            $all_refcol_to_col_dict, { 'FK_ID' => { 'ID' => 'REFID' } },
            'all_refcol_to_col_dict'
        ),
        'all_refcol_to_col_dict correctly determined'
    );
    ok( eq_hash(
            $all_refcol_lists, { 'FK_ID' => ['ID'] },
            'all_refcol_lists'
        ),
        'all_refcol_lists correctly determined'
    );
    is( $fkey_self_ref, 'REFID',
        'foreign key column REFID identified as reference field for self-reference'
    );

}
elsif ( $probe_class_short eq 'Postgres' ) {
    is_deeply(
        \%h,
        {   'ud_dt_unique' => {
                'character' => [ [ 'ud', 3 ] ],
                'date'      => [ [ 'dt', '2011-04-15' ] ]
            }
        },
        'unique constraint columns information correctly determined'
    );

    my %pkey_info = %{ $probe->unique_columns_with_max(1) };
    ok( eq_hash(
            \%pkey_info, { 'pkey' => { 'integer' => [ [ 'id', 5 ] ] } }
        ),
        'pkey column information correctly determined'
    );

    my $fkey_tables_ref = $probe->fkey_name_to_fkey_table();

    ok( eq_hash( $fkey_tables_ref, { 'fk_id' => $table } ),
        'correct foreign key determined' );

    my $all_refcol_to_col_dict =
        $probe->fkey_referenced_cols_to_referencing_cols( $fkey_tables_ref,
        ['id'] );

    my $all_refcol_lists =
        $probe->fkey_referenced_cols( $fkey_tables_ref, ['id'] );

    my $fkey_self_ref =
        @{ $probe->get_self_reference( $fkey_tables_ref, 'id' ) }[1];

    ok( eq_hash(
            $all_refcol_to_col_dict, { 'fk_id' => { 'id' => 'refid' } },
            'all_refcol_to_col_dict'
        ),
        'all_refcol_to_col_dict correctly determined'
    );
    ok( eq_hash(
            $all_refcol_lists, { 'fk_id' => ['id'] },
            'all_refcol_lists'
        ),
        'all_refcol_lists correctly determined'
    );
    is( $fkey_self_ref, 'refid',
        'foreign key column refid identified as reference field for self-reference'
    );

}
elsif ( $probe_class_short eq 'SQLite' ) {

    #note: in SQLite, dates are texts, therefore the max of the dates
    #is the maximum length of date texts, which is obviously nonsensical,
    #but can only be improved when trying to determine that a column
    #of type TEXT is meant to contain dates
    is_deeply(
        \%h,
        {   "sqlite_autoindex_${table}_1" =>
                { 'TEXT' => [ [ 'ud', 3 ], [ 'dt', '10' ] ] }
        },
        'unique constraint columns information correctly determined'
    );

    my %pkey_info = %{ $probe->unique_columns_with_max(1) };
    ok( eq_hash(
            \%pkey_info, { 'pkey' => { 'INTEGER' => [ [ 'id', 5 ] ] } }
        ),
        'pkey column information correctly determined'
    );

    my $fkey_tables_ref = $probe->fkey_name_to_fkey_table();

    ok( eq_hash( $fkey_tables_ref, { '0' => $table } ),
        'correct foreign key determined' );

    my $all_refcol_to_col_dict =
        $probe->fkey_referenced_cols_to_referencing_cols( $fkey_tables_ref,
        ['id'] );

    my $all_refcol_lists =
        $probe->fkey_referenced_cols( $fkey_tables_ref, ['id'] );

    my $fkey_self_ref =
        @{ $probe->get_self_reference( $fkey_tables_ref, 'id' ) }[1];

    ok( eq_hash(
            $all_refcol_to_col_dict, { '0' => { 'id' => 'refid' } },
            'all_refcol_to_col_dict'
        ),
        'all_refcol_to_col_dict correctly determined'
    );
    ok( eq_hash( $all_refcol_lists, { '0' => ['id'] }, 'all_refcol_lists' ),
        'all_refcol_lists correctly determined' );
    is( $fkey_self_ref, 'refid',
        'foreign key column refid identified as reference field for self-reference'
    );

}

#disconnect
END {
    if ($dbh) {
        $dbh->disconnect or croak $dbh->errstr;
    }
}

END {
    if ($dbh) {
        $dbh->do("DROP TABLE $table");
    }
}
