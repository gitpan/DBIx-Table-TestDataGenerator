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

my $table_non_num_pkey = 'test_testdatagenerator_nn_pkey';

my ( $dsn, $dbuser, $dbpwd ) =
    ( $ENV{'TDG_DSN'}, $ENV{'TDG_USER'}, $ENV{'TDG_PWD'} );

my $dbh =
    defined $dsn
    ? DBI->connect( $dsn, $dbuser, $dbpwd )
    : $db_driver_utils->get_in_memory_dbh();

my $probe_class_short = $db_driver_utils->db_driver_name($dbh);
my $probe_class =
    "DBIx::Table::TestDataGenerator::TableProbe::$probe_class_short";

my $generator = DBIx::Table::TestDataGenerator->new(
    dbh      => $dbh,
    schema   => $probe_class_short eq 'Oracle' ? $dbuser : 'public',
    table    => $table_non_num_pkey
);

load_class($probe_class);

my ( $probe, $initial_num_records_nnp );

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

########### TEST TABLE, NON NUMERIC PKEY ##########################

#create test table
sub create_test_table_non_num_pkey {
    my $sql;
    if ( $probe_class_short eq 'SQLite' ) {
        $sql = <<"END_SQL";
CREATE TABLE $table_non_num_pkey (
  refid INTEGER,
  ud TEXT PRIMARY KEY,
  dt DATE
)
END_SQL
    }
    else {
        $sql = <<"END_SQL";
CREATE TABLE $table_non_num_pkey (
  refid INTEGER,
  ud VARCHAR(100) PRIMARY KEY,
  dt DATE
)
END_SQL

    }
    $dbh->do($sql);
    return;
}

sub fill_test_table_non_num_pkey {
    $dbh->do(
        "INSERT INTO $table_non_num_pkey (refid, ud, dt) VALUES (1,'A','12.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $table_non_num_pkey (refid, ud, dt) VALUES (1,'B1','12.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $table_non_num_pkey (refid, ud, dt) VALUES (1,'BB','13.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $table_non_num_pkey (refid, ud, dt) VALUES (4,'CCC','15.04.2011')"
    );
    $dbh->do(
        "INSERT INTO $table_non_num_pkey (refid, ud, dt) VALUES (4,'X','11.04.2011')"
    );
    $initial_num_records_nnp = $probe->num_records();
    return;
}

################################## TEST TABLES SETUP #########################
$probe = $probe_class->new(
    dbh    => $dbh,
    schema => $dbuser,
    table  => $table_non_num_pkey
);

create_test_table_non_num_pkey();
fill_test_table_non_num_pkey();

####################################### TESTS ################################

### test non numeric pkey ###
#test: non numeric pkey, add ten rows
inflate( $initial_num_records_nnp + 10, 5 );
is( $probe->num_records(),
    $initial_num_records_nnp + 10,
    "testing table $table_non_num_pkey with non numeric pkey, added 10 records"
);

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
                $dbh->do(
                    "DROP TABLE $table_non_num_pkey CASCADE CONSTRAINTS");
            }
            if ( $probe_class_short eq 'Postgres' ) {
                $dbh->do("DROP TABLE $table_non_num_pkey CASCADE");
            }
            if ( $probe_class_short eq 'SQLite' ) {
                $dbh->do("DROP TABLE $table_non_num_pkey");
            }
        }
    }
}
