#!perl

use 5.006;
use strict;
use warnings;

use version; our $VERSION = qv('0.0.1_1');

use Readonly;

Readonly my $RANDOM_SEED => 42;

use Test::More;
use Test::Exception;

use DBI;
use Class::Load qw (load_class);
use DBIx::Table::TestDataGenerator;
use DBIx::Table::TestDataGenerator::DBDriverUtils;
plan tests => 20;
my $test_label;

my ( $generator, $probe );

#note: Oracle converts values of columns in system tables to uppercase,
#e.g. all_cons_columns.table_name, user_tab_columns.table_name,
#all_constraints.owner.
#PostgreSQL does not do it, it needs lowercase table names (?).
my $target_table       = 'test_testdatagenerator';
my $ref_table          = 'test_testdatagenerator_ref';
my $table_non_num_pkey = 'test_testdatagenerator_nn_pkey';

my ( $dsn, $dbuser, $schema, $dbpwd ) =
    ( $ENV{'TDG_DSN'}, $ENV{'TDG_USER'}, $ENV{'TDG_SCHEMA'},
    $ENV{'TDG_PWD'} );

my $db_driver_utils = DBIx::Table::TestDataGenerator::DBDriverUtils->new();

my $dbh =
    defined $dsn
    ? DBI->connect( $dsn, $dbuser, $dbpwd )
    : $db_driver_utils->get_in_memory_dbh();

my $probe_class_short = $db_driver_utils->db_driver_name($dbh);
my $probe_class =
    "DBIx::Table::TestDataGenerator::TableProbe::$probe_class_short";

load_class($probe_class);

my $database =
    DBIx::Table::TestDataGenerator::DBDriverUtils->get_database($dbh);

$generator = DBIx::Table::TestDataGenerator->new(
    dbh      => $dbh,
    schema   => $schema,
    table    => $target_table
);

my ($initial_num_records);

#convenience method to reduce typing
sub inflate {
    my ($target_size,  $num_random, $max_tree_depth,
        $min_children, $min_roots
    ) = @_;
    return $generator->create_testdata(
        target_size    => $target_size,
        num_random     => $num_random,
        max_tree_depth => $max_tree_depth,
        min_children   => $min_children,
        min_roots      => $min_roots,
        seed           => $RANDOM_SEED
    );

}

################################### TARGET TABLE UTILS ##########################

#create target table
sub create_target_table {
    my $sql = <<"END_SQL";
CREATE TABLE $target_table (
  id  INTEGER NOT NULL,
  refid INTEGER,
  ud VARCHAR(100),
  dt DATE
)
END_SQL

    $dbh->do($sql);
    return;
}

sub create_target_table_sqlite {
    my $sql = <<"END_SQL";
CREATE TABLE $target_table (
  id  INTEGER NOT NULL,
  refid INTEGER,
  ud TEXT,
  dt TEXT
)
END_SQL

    $dbh->do($sql);
    return;
}

#create target table with primary key, needed for the SQLite case (see below)
sub create_target_table_with_pkey_sqlite {
    my $sql = <<"END_SQL";
CREATE TABLE $target_table (
  id  INTEGER PRIMARY KEY,
  refid INTEGER,
  ud TEXT,
  dt TEXT
)
END_SQL

    $dbh->do($sql);
    return;
}

sub create_target_table_with_pkey_and_uq_key_sqlite {
    my $sql = <<"END_SQL";
CREATE TABLE $target_table (
  id  INTEGER PRIMARY KEY,
  refid INTEGER,
  ud TEXT,
  dt TEXT,
  UNIQUE (ud,dt)
)
END_SQL

    $dbh->do($sql);
    return;
}

sub create_target_table_with_pkey_and_uq_key_selfref_sqlite {
    my $sql = <<"END_SQL";
CREATE TABLE $target_table (
  id  INTEGER PRIMARY KEY,
  refid INTEGER,
  ud TEXT,
  dt TEXT,
  UNIQUE (ud,dt),
  CONSTRAINT fkey FOREIGN KEY (refid) REFERENCES $target_table (id)
)
END_SQL

    $dbh->do($sql);
    return;
}

sub create_target_table_with_pkey_uq_key_fkey_sqlite {
    my $sql = <<"END_SQL";
CREATE TABLE $target_table (
  id  INTEGER PRIMARY KEY,
  refid INTEGER,
  ud TEXT,
  dt TEXT,
  UNIQUE (ud,dt),
  CONSTRAINT fkey FOREIGN KEY (refid) REFERENCES $ref_table (k)
)
END_SQL

    $dbh->do($sql);
    return;
}

sub fill_target_table {
    $dbh->do(
        "INSERT INTO $target_table (id, refid, ud, dt) VALUES (1,1,'A','12.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $target_table (id, refid, ud, dt) VALUES (2,1,'B1','12.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $target_table (id, refid, ud, dt) VALUES (3,1,'BB','13.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $target_table (id, refid, ud, dt) VALUES (4,4,'CCC','15.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $target_table (id, refid, ud, dt) VALUES (5,4,'X','11.04.2011')"
    );
    $initial_num_records = $probe->num_records();
    return;
}

sub truncate_target_table {
    if ( $probe_class_short eq 'SQLite' ) {
        $dbh->do("DELETE FROM $target_table");
    }
    else {
        $dbh->do("TRUNCATE TABLE $target_table");
    }
    return;
}

sub reset_target_table {
    truncate_target_table();
    fill_target_table();
    return;
}

sub add_pkey_constraint {

#note: primary key constraints cannot be added to an existing SQLite table,
#one needs to recreate the table defining the primary key in the table definition
    if ( $probe_class_short eq 'SQLite' ) {
        $dbh->do("DROP TABLE $target_table");
        create_target_table_with_pkey_sqlite();
        fill_target_table();
    }
    else {
        my $sql = <<"END_SQL";
ALTER TABLE $target_table
ADD CONSTRAINT pk_$target_table PRIMARY KEY (id)
END_SQL

        $dbh->do($sql);
    }
    return;
}

sub add_unique_constraint {
    if ( $probe_class_short eq 'SQLite' ) {
        $dbh->do("DROP TABLE $target_table");
        create_target_table_with_pkey_and_uq_key_sqlite();
        fill_target_table();
    }
    else {
        my $sql = <<"END_SQL";
ALTER TABLE $target_table
ADD CONSTRAINT uq_$target_table UNIQUE(ud,dt)
END_SQL

        $dbh->do($sql);
    }
    return;
}

sub add_self_reference {
    if ( $probe_class_short eq 'SQLite' ) {
        $dbh->do("DROP TABLE $target_table");
        create_target_table_with_pkey_and_uq_key_selfref_sqlite();
        fill_target_table();
    }
    else {
        my $sql = <<"END_SQL";
ALTER TABLE $target_table
ADD CONSTRAINT fk_$target_table
  FOREIGN KEY (refid)
  REFERENCES $target_table (id)
END_SQL

        $dbh->do($sql);
    }
    return;
}

################################### REF TABLE UTILS ##########################

#create table referenced by foreign key relation
sub create_ref_table {
    my $sql = <<"END_SQL";
CREATE TABLE $ref_table (
  k INTEGER NOT NULL,
  a VARCHAR(100)
)
END_SQL

    $dbh->do($sql);
    return;
}

#for the SQLite case (see below)
sub create_ref_table_with_pkey {
    my $sql = <<"END_SQL";
CREATE TABLE $ref_table (
  k INTEGER PRIMARY KEY,
  a VARCHAR(100)
)
END_SQL

    $dbh->do($sql);
    return;
}

sub add_pkey_constraint_ref_table {

#note: primary key constraints cannot be added to an existing SQLite table,
#one needs to recreate the table defining the primary key in the table definition
    if ( $probe_class_short eq 'SQLite' ) {
        $dbh->do("DROP TABLE $ref_table");
        create_ref_table_with_pkey();
    }
    else {
        my $sql = <<"END_SQL";
ALTER TABLE $ref_table
ADD CONSTRAINT pk_$ref_table PRIMARY KEY (k)
END_SQL

        $dbh->do($sql);
    }
    return;
}

sub fill_ref_table {
    $dbh->do("INSERT INTO $ref_table (k, a) VALUES (1,'r_A')");
    $dbh->do("INSERT INTO $ref_table (k, a) VALUES (2,'r_B')");
    $dbh->do("INSERT INTO $ref_table (k, a) VALUES (3,'r_C')");
    $dbh->do("INSERT INTO $ref_table (k, a) VALUES (4,'r_D')");
    return;
}

sub define_ref_table {
    create_ref_table();
    add_pkey_constraint_ref_table();
    fill_ref_table();
    return;
}

sub add_fkey_constraint_ref_table {
    if ( $probe_class_short eq 'SQLite' ) {
        $dbh->do("DROP TABLE $target_table");
        create_target_table_with_pkey_uq_key_fkey_sqlite();
        fill_target_table();
    }
    else {
        my $sql = <<"END_SQL";
ALTER TABLE $target_table
ADD CONSTRAINT fk_$ref_table
  FOREIGN KEY (refid)
  REFERENCES $ref_table (k)
END_SQL

        $dbh->do($sql);
    }
    return;
}

sub remove_fkey_constraint_ref_table {
    my $sql = <<"END_SQL";
ALTER TABLE $target_table
DROP CONSTRAINT fk_$ref_table
END_SQL

    $dbh->do($sql);
    return;
}

################################## TEST TABLES SETUP #########################

if ( $probe_class_short eq 'SQLite' ) {
    create_target_table_sqlite();
}
else {
    create_target_table();
}
$probe = $probe_class->new(
    dbh      => $dbh,
    database => $database,
    schema   => $schema,
    table    => $target_table
);
define_ref_table();

####################################### TESTS ################################

my $col_names_ref = $probe->column_names();
my $col_widths_ref = [ (10) x @{$col_names_ref} ];

sub print_result {
    my ($label) = @_;

    #comment the following line to see the resulting records, preferably
    #redirecting the output to a file
    #return;
    print "$label:\n\n";
    $probe->print_table( $col_names_ref, $col_widths_ref );
    print "#" x 100;
    print "\n";
    return;
}

### test for invalid input parameters or invalid test table state ###

#test: num_random too small
throws_ok {
    inflate( $initial_num_records, 1 );
}
qr/num_random must be greater or equal to two/,
    'num_random must be greater or equal to two';

#test: max_tree_depth too small
throws_ok {
    inflate( $initial_num_records, 5, 1 );
}
qr/max_tree_depth must be greater or equal to two/,
    'max_tree_depth must be greater or equal to two';

#test: empty table
truncate_target_table();
throws_ok {
    inflate( $initial_num_records, 5 );
}
qr/target table $target_table must not be empty/, 'target table must not be empty';

### no self-ref handling requested ###

#test: number of requested records equal to current number
#of records
reset_target_table();
print_result('original table');
inflate( $initial_num_records, 5 );
is( $probe->num_records(), $initial_num_records,
    "requested $initial_num_records records, already $initial_num_records records in table"
);

#test: no constraints defined in table, add ten rows
$test_label = 'no constraints, no self-ref requested, added 10 records';
reset_target_table();
inflate( $initial_num_records + 10, 2 );
is( $probe->num_records(), $initial_num_records + 10, $test_label );
print_result($test_label);

#test: reset the table, create a primary key, add ten rows,
#do not handle self-reference
$test_label = 'pkey constraint, no self-ref requested, added 10 records';
reset_target_table();
add_pkey_constraint();
inflate( $initial_num_records + 10, 50 );
is( $probe->num_records(), $initial_num_records + 10, $test_label );
print_result($test_label);

#test: reset the table, create a primary and a unique key, add
#ten rows, do not handle self-reference
$test_label =
    'pkey and unique key constraint, no self-ref requested, added 10 records';
reset_target_table();
add_unique_constraint();
inflate( $initial_num_records + 10, 50 );
is( $probe->num_records(), $initial_num_records + 10, $test_label );
print_result($test_label);

#test: reset table, create a primary and a unique key as well as a foreign key
#from the table to another table, do not request handling self-references
$test_label =
    'pkey, unique and fkey constraints, no self-ref requested, added 10 records';
reset_target_table();
add_fkey_constraint_ref_table();
inflate( $initial_num_records + 10, 50 );
is( $probe->num_records(), $initial_num_records + 10, $test_label );
print_result($test_label);

### specifying some, but not all parameters needed to handle self-reference#####

#test: only min_roots missing from self-reference handling request
reset_target_table();
throws_ok {
    inflate( $initial_num_records + 10, 50, 2, 1 );
}
qr/min_roots parameter is missing/, 'min_roots missing';

#test: only min_children missing from self-reference handling request
reset_target_table();
throws_ok {
    inflate( $initial_num_records + 10, 50, 2, undef, 1 );
}
qr/min_children parameter is missing/, 'min_children missing';

#test: only max_tree_depth missing from self-reference handling request
reset_target_table();
throws_ok {
    inflate( $initial_num_records + 10, 50, undef, 2, 3 );
}
qr/max_tree_depth parameter is missing/, 'max_tree_depth missing';

#test: only max_tree_depth defined for self-reference handling request
reset_target_table();
throws_ok {
    inflate( $initial_num_records + 10, 50, 2 );
}
qr/(min_children|min_roots) parameter is missing/,
    'min_children or min_roots missing';

#test: only min_roots defined for self-reference handling request
reset_target_table();
throws_ok {
    inflate( $initial_num_records + 10, 50, undef, undef, 2 );
}
qr/(min_children|max_tree_depth) parameter is missing/,
    'min_children or max_tree_depth missing';

### self-ref handling requested ###

#test: no constraints defined in table, request handling of (non-existent)
#self-constraint, add ten rows
$test_label =
    'no constraints, (non-existent) self-ref requested, added 10 records';
reset_target_table();
inflate( $initial_num_records + 10, 5, 2, 2, 5 );
is( $probe->num_records(), $initial_num_records + 10, $test_label );
print_result($test_label);

#test: pkey, unique key and fkey as self-reference defined in table,
#add 10 rows
reset_target_table();

#remove foreign key to other table(not necessary for SQLite since we need to recreate
#the table in any case)
$test_label =
    'all constraints, handling of self-ref requested and possible, added 10 records';
if ( $probe_class_short ne 'SQLite' ) {
    remove_fkey_constraint_ref_table();
}
add_self_reference();
inflate( $initial_num_records + 10, 5, 2, 2, 2 );
is( $probe->num_records(), $initial_num_records + 10, $test_label );
print_result($test_label);

#add 50 rows, min_children = 0, min_roots > num_roots
$test_label =
    'all constraints, handling of self-ref requested and possible, added 50 records';
reset_target_table();

inflate( $initial_num_records + 50, 5, 2, 0, 6 );
is( $probe->num_records(), $initial_num_records + 50, $test_label );
print_result($test_label);

#test: pkey, unique key and fkey as self-reference defined in table,
#add 10 rows, min_children = 1, min_roots > num_roots
$test_label =
    'all constraints, handling of self-ref requested and possible, added 10 records';
reset_target_table();

inflate( $initial_num_records + 10, 5, 2, 1, 6 );
is( $probe->num_records(), $initial_num_records + 10, $test_label );
print_result($test_label);

#test: pkey, unique key and fkey as self-reference defined in table,
#add 10 rows, num_random = 2, min_children = 1, min_roots > num_roots
$test_label =
    'all constraints, handling of self-ref requested and possible, added 10 records';
reset_target_table();

inflate( $initial_num_records + 10, 2, 2, 1, 6 );
is( $probe->num_records(), $initial_num_records + 10, $test_label );
print_result($test_label);

#test: pkey, unique key and fkey as self-reference defined in table,
#add 50 rows, min_roots > num_roots
$test_label =
    'all constraints, handling of self-ref requested and possible, added 50 records';
reset_target_table();

inflate( $initial_num_records + 50, 5, 2, 2, 6 );
is( $probe->num_records(), $initial_num_records + 50, $test_label );
ok( $probe->num_roots( 'id', 'refid' ) >= 6, 'check number of roots' );
print_result($test_label);

####################################### CLEANUP ###############################

#disconnect
END {
    if ($dbh) {
        $dbh->disconnect or croak $dbh->errstr;
    }
}

END {
    if ($dbh) {

      # $probe->print_table( [ 'id', 'refid', 'ud', 'dt' ], [ 4, 6, 8, 10 ] );
        if ( defined $probe_class_short ) {
            if ( $probe_class_short eq 'Oracle' ) {
                $dbh->do("DROP TABLE $target_table CASCADE CONSTRAINTS");
                $dbh->do("DROP TABLE $ref_table CASCADE CONSTRAINTS");
            }
            if ( $probe_class_short eq 'Postgres' ) {
                $dbh->do("DROP TABLE $target_table CASCADE");
                $dbh->do("DROP TABLE $ref_table CASCADE");
            }
            if ( $probe_class_short eq 'SQLite' ) {
                $dbh->do("DROP TABLE $target_table");
                $dbh->do("DROP TABLE $ref_table");
            }
        }
    }
}
