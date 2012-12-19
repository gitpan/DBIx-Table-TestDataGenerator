package DBIx::Table::TestDataGenerator::DBDriverUtils;
use Moo;

use strict;
use warnings;

use Carp;

use DBI::Const::GetInfoType;
use DBD::SQLite;

use Readonly;
Readonly my $IN_MEMORY_DSN => 'dbi:SQLite:dbname=:memory:';

sub get_in_memory_dsn {
    return $IN_MEMORY_DSN;
}

sub get_dbh {
    my ( $self, $dsn, $user, $password ) = @_;
    my $dbh = DBI->connect( $dsn, $user, $password );

    $self->check_db_handle($dbh);
    return $dbh;
}

#Todo: check the following method, if / else criterion does not seem to make sense
sub db_driver_name {
    my ( $self, $dsn ) = @_;
    if ( defined $dsn && $dsn =~ /\w/ ) {
        return ( DBI->parse_dsn($dsn) )[1];
    }
    else {
        return 'SQLite';
    }
}

sub check_db_handle {
    my ( $self, $dbh ) = @_;
    croak 'No working database connection provided.' unless $dbh->ping();
    return;
}

sub get_database {
    my ( $self, $dbh ) = @_;
    return $dbh->get_info( $GetInfoType{SQL_DATABASE_NAME} );
}

1;    # End of DBIx::Table::TestDataGenerator::DBDriverUtils

__END__

=pod

=head1 NAME

DBIx::Table::TestDataGenerator::DBDriverUtils - Common DBI database handle methods

=head1 SUBROUTINES/METHODS

=head2 get_in_memory_dsn

Arguments: none

Returns the dsn for an in memory SQLite database, see comment for get_in_memory_dbh.

=head2 get_in_memory_dbh

Arguments: none

Returns a database handle for a new in-memory database. This is needed for running the module install tests in case the TDG_... environment variables have not been set. The in-memory database is a SQLite database provided by the wonderful DBD::Sqlite module.

=head2 get_dbh

Arguments: DBI data source name, user and password

Returns the corresponding database handle if a connection can be established, croaks otherwise. If the data source name has not been passed, an in-memory SQLite database is created and the returned database handle points to it.

=head2 db_driver_name 

Argument: database handle $dbh

Return Value: db driver name determined from $dbh. Note that the corresponding TableProbe role impersonating class must have this name as last part of it package name.

=head2 check_db_handle

Argument: database handle $dbh

Pings database with handle $dbh to check if it is available, aborts if not.

=head2 get_database

Argument: database handle $dbh

Returns the database name as determined from the database handle.

=head1 AUTHOR

Jose Diaz Seng, C<< <josediazseng at gmx.de> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Jose Diaz Seng.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

