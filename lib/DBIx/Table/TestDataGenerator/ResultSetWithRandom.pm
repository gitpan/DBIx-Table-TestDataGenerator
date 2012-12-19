package DBIx::Table::TestDataGenerator::ResultSetWithRandom;

use strict;
use warnings;

use Carp;

use DBIx::Class::Helper::ResultSet::Random;

use parent "DBIx::Class::ResultSet";

__PACKAGE__->load_components("Helper::ResultSet::Random");

1;    # End of DBIx::Table::TestDataGenerator::ResultSetWithRandom

__END__

=pod

=head1 NAME

DBIx::Table::TestDataGenerator::ResultSetWithRandom - Helper class enabling random selections

=head1 DESCRIPTION

We do not know at compile time which ResultSet classes will exist. In order to enable making random selections from ResultSet objects, one can bless them into the current class having a rand() method.

=head1 AUTHOR

Jose Diaz Seng, C<< <josediazseng at gmx.de> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Jose Diaz Seng.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

