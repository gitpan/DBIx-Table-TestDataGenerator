package Module::Build::Custom;

use 5.006;
use strict;
use warnings;
use Module::Build;

my $custom_build = Module::Build->subclass(
    class => "Module::Build::Custom",
    code => <<'SUBCLASS' );

sub ACTION_authortest {
    my ($self) = @_;

    $self->depends_on('build');
    $self->depends_on('manifest');
    $self->depends_on('distmeta');

    $self->test_files( qw( t xt ) );
    $self->recursive_test_files(1);

    $self->depends_on('test');

    return;
}
SUBCLASS

my $builder = $custom_build->new(
    module_name        => 'DBIx::Table::TestDataGenerator',
    license            => 'perl',
    dist_author        => q{José Diaz Seng <josediazseng@gmx.de>},
    dist_version_from  => 'lib/DBIx/Table/TestDataGenerator.pm',
    build_requires     => { 'Test::More' => 0, },
    requires           => { 'perl' => 5.006, },
    add_to_cleanup     => ['DBIx-Table-TestDataGenerator-*'],
    create_makefile_pl => 'traditional',
    configure_requires => { 'Module::Build' => 0.40 },
);

$builder->create_build_script();
