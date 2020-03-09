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

import os

import pgdump_tables

MIN_ROWS_NUMBER = 100


if __name__ == '__main__':
    # Establish database connection
    connection = pgdump_tables.Connection(hostname=os.environ.get('DB_HOST', 'localhost'),
                                          port=os.environ.get('DB_PORT', 5432),
                                          database=os.environ.get('DB_DATABASE', 'postgres'),
                                          username=os.environ.get('DB_USERNAME', 'root'),
                                          password=os.environ.get('DB_PASSWORD', 'password'))
    connection.connect()
    # Get all the tables name
    tables = connection.list_tables_name()
    # Cycle each table and count records
    for index, table in enumerate(tables):
        cursor = connection.cursor()
        cursor.execute("SELECT "
                       "  COUNT(*) "
                       "FROM {TABLE_NAME}".format(TABLE_NAME=table[0]))
        data = cursor.fetchall()
        # Include only tables with the minimum rows count
        if data[0][0] >= MIN_ROWS_NUMBER:
            print('{INDEX},{TABLE_NAME},{RECORDS}'.format(INDEX=index + 1,
                                                          TABLE_NAME=table[0],
                                                          RECORDS=data[0][0]))
    # Close cursor and connection
    cursor.close()
    connection.close()
