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

from .cursor import Cursor


class Connection(object):
    """
    Connection object used to interact with a Postgres database
    """
    def __init__(self,
                 hostname: str,
                 port: int,
                 database: str,
                 username: str,
                 password: str) -> None:
        """
        Initialize the database connection
        :param hostname: postgres host
        :param port: postgres port number
        :param database: database name
        :param username: user name
        :param password: password for the user
        :return: None
        """
        self.hostname = hostname
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self._connection = None

    def connect(self) -> None:
        """
        Establish the database connection
        :return: None
        """
        self._connection = psycopg2.connect(user=self.username,
                                            password=self.password,
                                            host=self.hostname,
                                            port=self.port,
                                            database=self.database)

    def close(self) -> None:
        """
        Close the database connection
        :return: None
        """
        if self._connection:
            self._connection.close()
            self._connection = None

    def cursor(self) -> Cursor:
        """
        Create a new database cursor
        :return: psycopg2 cursor
        """
        return Cursor(self._connection.cursor())

    def execute(self,
                statement: str,
                *args: list) -> Cursor:
        """
        Execute a statement on the current database connection and returns a database cursor
        :param statement: statement to execute
        :param args: optional arguments for the statement
        :return: psycopg2 cursor
        """
        cursor = self.cursor()
        cursor.execute(statement, args)
        return cursor

    def list_tables_name(self) -> list:
        """
        Return the list of all the tables in the database connection
        :return: a list with all the tables names
        """
        cursor = self.execute("SELECT "
                              "  table_name "
                              "FROM information_schema.tables "
                              "WHERE table_schema = 'public'"
                              "ORDER BY table_name")
        return cursor.fetchall()
