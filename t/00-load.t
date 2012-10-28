#!perl -T

use Test::More tests => 3;

BEGIN {
    use_ok( 'DBIx::Table::TestDataGenerator' ) || print "Bail out!\n";
    use_ok( 'DBIx::Table::TestDataGenerator::TableProbe' ) || print "Bail out!\n";
    use_ok( 'DBIx::Table::TestDataGenerator::TreeUtils' ) || print "Bail out!\n";    
}

diag( "Testing DBIx::Table::TestDataGenerator $DBIx::Table::TestDataGenerator::VERSION, Perl $], $^X" );
