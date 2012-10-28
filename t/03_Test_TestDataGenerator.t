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

plan tests => 1;

my $db_driver_utils = DBIx::Table::TestDataGenerator::DBDriverUtils->new();

my $table       = 'tdg_target';
my $ref_table_1 = 'tdg_ref1';
my $ref_table_2 = 'tdg_ref2';

my ( $dsn, $dbuser, $dbpwd ) =
    ( $ENV{'TDG_DSN'}, $ENV{'TDG_USER'}, $ENV{'TDG_PWD'} );

my $dbh =
    defined $dsn
    ? DBI->connect( $dsn, $dbuser, $dbpwd )
    : $db_driver_utils->get_in_memory_dbh();

my $probe_class_short = $db_driver_utils->db_driver_name($dbh);
my $probe_class =
    "DBIx::Table::TestDataGenerator::TableProbe::$probe_class_short";

my $database = $db_driver_utils->get_database($dbh);
my $schema = $probe_class_short eq 'Oracle' ? $dbuser : 'public';

my $generator = DBIx::Table::TestDataGenerator->new(
    dbh      => $dbh,
    schema   => $schema,
    table    => $table
);

load_class($probe_class);

my $probe = $probe_class->new(
    dbh      => $dbh,
    database => $database,
    schema   => $schema,
    table    => $table
);

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

###### TEST TABLE REFERENCING ITSELF AND TWO OTHER TABLES ###################

#create test tables
sub create_test_tables {
    my $sql1 = <<"END_SQL";
CREATE TABLE $table (
  id INTEGER,
  ref1a INTEGER,
  ref1b INTEGER,
  ref2a INTEGER,
  ref2b INTEGER,
  ud VARCHAR(100)
)
END_SQL

    my $sql2 = <<"END_SQL1";
CREATE TABLE $ref_table_1 (
  id1a INTEGER,
  id1b INTEGER,
  ud VARCHAR(100)
)
END_SQL1

    my $sql3 = <<"END_SQL2";
CREATE TABLE $ref_table_2 (
  id2a INTEGER,
  id2b INTEGER,
  ud VARCHAR(100)
)
END_SQL2

    $dbh->do($_) for ( "$sql1", "$sql2", "$sql3" );
    return;
}

sub add_constraints {
    my $sql1 = <<"END_SQL";
ALTER TABLE $table
ADD CONSTRAINT pk_table PRIMARY KEY (id)
END_SQL

    my $sql2 = <<"END_SQL";
ALTER TABLE $ref_table_1
ADD CONSTRAINT pk_ref_table_1 PRIMARY KEY (id1a, id1b)
END_SQL

    my $sql3 = <<"END_SQL";
ALTER TABLE $ref_table_2
ADD CONSTRAINT pk_ref_table_2 PRIMARY KEY (id2a, id2b)
END_SQL

    my $sql4 = <<"END_SQL";
ALTER TABLE $table
ADD CONSTRAINT fk_table_1
  FOREIGN KEY (ref1a, ref1b)
  REFERENCES $ref_table_1 (id1a, id1b)
END_SQL

    my $sql5 = <<"END_SQL";
ALTER TABLE $table
ADD CONSTRAINT fk_table_2
  FOREIGN KEY (ref2a, ref2b)
  REFERENCES $ref_table_2 (id2a, id2b)
END_SQL

    $dbh->do($_) for ( "$sql1", "$sql2", "$sql3", "$sql4", "$sql5" );
    return;
}

#create test tables in SQLite case
sub create_test_tables_sqlite {
    my $sql1 = <<"END_SQL";
CREATE TABLE $table (
  id INTEGER PRIMARY KEY,
  ref1a INTEGER,
  ref1b INTEGER,
  ref2a INTEGER,
  ref2b INTEGER,
  ud TEXT,
  FOREIGN KEY (ref1a, ref1b) REFERENCES $ref_table_1 (id1a, id1b),
  FOREIGN KEY (ref2a, ref2b) REFERENCES $ref_table_2 (id2a, id2b)
)
END_SQL

    my $sql2 = <<"END_SQL1";
CREATE TABLE $ref_table_1 (
  id1a INTEGER,
  id1b INTEGER,
  ud TEXT,
  PRIMARY KEY (id1a, id1b)
)
END_SQL1

    my $sql3 = <<"END_SQL2";
CREATE TABLE $ref_table_2 (
  id2a INTEGER,
  id2b INTEGER,
  ud TEXT,
  PRIMARY KEY (id2a, id2b)
)
END_SQL2

    $dbh->do($_) for ( "$sql2", "$sql3", "$sql1" );
    return;
}

sub fill_test_tables {

    #ref_table_1
    $dbh->do("INSERT INTO $ref_table_1 (id1a, id1b, ud) VALUES (1,2,'a')");
    $dbh->do("INSERT INTO $ref_table_1 (id1a, id1b, ud) VALUES (2,3,'b')");
    $dbh->do("INSERT INTO $ref_table_1 (id1a, id1b, ud) VALUES (3,4,'c')");
    $dbh->do("INSERT INTO $ref_table_1 (id1a, id1b, ud) VALUES (4,5,'d')");

    #ref_table_2
    $dbh->do(
        "INSERT INTO $ref_table_2 (id2a, id2b, ud) VALUES (11,111,'aaa')");
    $dbh->do(
        "INSERT INTO $ref_table_2 (id2a, id2b, ud) VALUES (12,112,'bbb')");
    $dbh->do(
        "INSERT INTO $ref_table_2 (id2a, id2b, ud) VALUES (13,113,'ccc')");
    $dbh->do(
        "INSERT INTO $ref_table_2 (id2a, id2b, ud) VALUES (14,114,'ddd')");

    #target table
    $dbh->do(
        "INSERT INTO $table (id, ref1a, ref1b, ref2a, ref2b, ud) VALUES (1,2,3,13,113,'x')"
    );
    $dbh->do(
        "INSERT INTO $table (id, ref1a, ref1b, ref2a, ref2b, ud) VALUES (2,4,5,11,111,'y')"
    );
    $dbh->do(
        "INSERT INTO $table (id, ref1a, ref1b, ref2a, ref2b, ud) VALUES (3,3,4,11,111,'z')"
    );
    $dbh->do(
        "INSERT INTO $table (id, ref1a, ref1b, ref2a, ref2b, ud) VALUES (4,1,2,12,112, 'u')"
    );
    $dbh->do(
        "INSERT INTO $table (id, ref1a, ref1b, ref2a, ref2b, ud) VALUES (5,1,2,14,114,'v')"
    );
    return;
}

################################## TEST TABLES SETUP #########################
if ( $probe_class_short eq 'SQLite' ) {
    create_test_tables_sqlite();
}
else {
    create_test_tables();
    add_constraints();
}
fill_test_tables();

####################################### TESTS ################################

### test non numeric pkey ###
#test: non numeric pkey, add ten rows
inflate( 70, 5 );
is( $probe->num_records(), 70, "testing table $table, added 65 records" );

# $probe->print_table(['id','ref1a','ref1b','ref2a','ref2b','ud'], [5,5,5,5,5,5]);

####################################### CLEANUP ###############################

#disconnect
END {
    if ($dbh) {
        $dbh->disconnect or croak $dbh->errstr;
    }
}

END {
    if ($dbh) {
        if ( defined $probe_class_short ) {
            if ( $probe_class_short eq 'Oracle' ) {
                $dbh->do("DROP TABLE $table CASCADE CONSTRAINTS");
                $dbh->do("DROP TABLE $ref_table_1 CASCADE CONSTRAINTS");
                $dbh->do("DROP TABLE $ref_table_2 CASCADE CONSTRAINTS");
            }
            if ( $probe_class_short eq 'Postgres' ) {
                $dbh->do("DROP TABLE $table CASCADE");
                $dbh->do("DROP TABLE $ref_table_1 CASCADE");
                $dbh->do("DROP TABLE $ref_table_2 CASCADE");
            }
            if ( $probe_class_short eq 'SQLite' ) {
                $dbh->do("DROP TABLE $table");
                $dbh->do("DROP TABLE $ref_table_1");
                $dbh->do("DROP TABLE $ref_table_2");
            }
        }
    }
}
