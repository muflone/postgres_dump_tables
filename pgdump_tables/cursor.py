##
#     Project: PostgreSQL Dump Tables
# Description: Export sample data from Postgres database tables
#      Author: Fabio Castelli (Muflone) <muflone@muflone.com>
#   Copyright: 2020 Fabio Castelli
#     License: GPL-3+
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
##

import psycopg2


class Cursor(object):
    """
    Cursor object to get data from a Postgres database
    """
    def __init__(self,
                 cursor: psycopg2.extensions.cursor) -> None:
        """
        Initialize the database cursor
        :param cursor: psycopg2 cursor
        """
        self._cursor = cursor

    def close(self) -> None:
        """
        Close the database cursor
        :return: None
        """
        if self._cursor:
            self._cursor.close()
            self._cursor = None

    def execute(self,
                statement: str,
                *args: list) -> None:

        """
        Execute a statement on the current database cursor
        :param statement: statement to execute
        :param args: optional arguments for the statement
        :return: None
        """
        return self._cursor.execute(statement, args)

    def fetchall(self) -> list:
        """
        Return a list of all the rows for the cursor
        :return: list of rows
        """
        return self._cursor.fetchall()
