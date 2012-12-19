use strict;
use warnings;

use Test::More;
use Test::Exception;

use DBIx::Table::TestDataGenerator;
use DBIx::Table::TestDataGenerator::TableProbe;
use DBIx::Table::TestDataGenerator::DBDriverUtils;
plan tests => 9;

my $db_driver_utils = DBIx::Table::TestDataGenerator::DBDriverUtils->new();

my $table = 'test_TDG';

my $dsn   = $db_driver_utils->get_in_memory_dsn();
my $user = my $password = q{};

my $probe = DBIx::Table::TestDataGenerator::TableProbe->new(
    dsn                   => $dsn,
    user                  => q{},
    password              => q{},
    table                 => $table,
    on_the_fly_schema_sql => 't/db/schema.sql',
);

$probe->dump_schema();

#test unique_columns_with_max
my %unique_constraints = %{ $probe->unique_columns_with_max(0) };

#test num_records
my $initial_num_records = $probe->num_records();
is( $initial_num_records, 5, 'check initial number of records' );

#test column_names
my @column_names_sorted = sort( @{ $probe->column_names() } );
is_deeply(
    \@column_names_sorted,
    [ 'dt', 'id', 'j', 'refid', 'ud' ],
    'correct column names'
);

#test random_record
my %ids;
my $num_samples    = 2**31-2;
my $cols           = [ 'dt', 'id', 'j', 'refid', 'ud' ];
for ( 1 .. $num_samples ) {
    my %r = %{ $probe->random_record( $table, $cols ) };
    $ids{ $r{id} }++;
    last if keys %ids == $initial_num_records;
}

#by choice of $num_samples, the probability of one of those pkeys
#missing is $num_samples / $max_signed_int
is( keys %ids, $initial_num_records, 'all pkeys found in random samples' );

#test num_roots
is( $probe->num_roots(), 2, 'checking number of roots' );

#test fkey_name_to_source
my $fkey_to_src = $probe->fkey_name_to_source();
is_deeply(
    $fkey_to_src,
    {
        'j'     => 'TestTdgRef',
        'refid' => 'TestTdg'
    },
    'foreign keys correctly determined'
);

#test fkey_referenced_cols_to_referencing_cols
my $refd_to_refng = $probe->fkey_referenced_cols_to_referencing_cols();
is_deeply(
    $refd_to_refng,
    {
        'refid' => { 'id' => 'refid' },
        'j'     => { 'i'  => 'j' }
    },
    'referenced to referencing foreign key constrained columns determined'
);

#test fkey_referenced_cols
my $fkey_refd_cols = $probe->fkey_referenced_cols();
is_deeply(
    $fkey_refd_cols,
    {
        'refid' => ['id'],
        'j'     => ['i']
    },
    'fkeys to lists of referenced constrained columns determined'
);

#test get_self_reference
my $self_ref_info = $probe->get_self_reference('id');
is_deeply(
    $self_ref_info,
    [ 'refid', 'refid' ],
    'self reference correctly determined'
);

#test selfref_tree
my $tree_ref = $probe->selfref_tree( 'id', 'refid' );
is_deeply(
    $tree_ref,
    { 1 => [ 1, 2, 3 ], 4 => [ 4, 5 ] },
    'self reference tree correctly determined'
);

