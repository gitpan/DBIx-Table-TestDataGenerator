use strict;
use warnings;

use Test::More;
use Test::Exception;

use DBIx::Table::TestDataGenerator;
use DBIx::Table::TestDataGenerator::DBDriverUtils;
plan tests => 2;

my $table = 'test_TDG';

my $db_driver_utils = DBIx::Table::TestDataGenerator::DBDriverUtils->new();

my $dsn = $db_driver_utils->get_in_memory_dsn();
my $user = my $password = q{};

my $generator = DBIx::Table::TestDataGenerator->new(
    dsn                   => $dsn,
    user                  => $user,
    password              => $password,
    on_the_fly_schema_sql => 't/db/schema.sql',
    table                 => $table,
);

my $target_size = 18;

$generator->create_testdata(
    target_size    => $target_size,
    num_random     => $target_size,
    max_tree_depth => 2,
    min_children   => 2,
    min_roots      => 6,
);

#use TableProbe to query resulting data
my $probe = $generator->probe;

#test resulting total number of records
is( $probe->num_records(), $target_size,
    "there are now $target_size records in the target table" );

#test number of roots
ok(
    $probe->num_roots( 'id', 'refid' ) >= 6,
    'the number of roots is at least 6'
);

my $col_names_ref = $probe->column_names();
my $col_widths_ref = [ (10) x @{$col_names_ref} ];
$probe->print_table( $col_names_ref, $col_widths_ref );
