import csv
import psycopg
from psycopg.sql import SQL, Identifier


def drop_table(psql_config, table_name='LDZ'):

    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()
    curr.execute(SQL('drop table if exists {}').format(Identifier(table_name)))
    conn.close()


def create_ldz_table(psql_config, table_name='LDZ'):
    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()
    try:
        curr.execute(
            SQL("""
                CREATE TABLE {} (
                    id serial PRIMARY KEY,
                    postcode varchar not null,
                    LDZ varchar not null
                )""").format(Identifier(table_name)))

    except psycopg.errors.DuplicateTable:
        pass

    conn.close()


def insert_ldz(psql_config, table_name='LDZ'):
    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    for dn in ['NG', 'NGN', 'SGN', 'WWU']:

        with open(f'./ldz/{dn}.csv', 'r') as file:
            data = list(csv.reader(file))[1:]

            with curr.copy(
                    SQL("COPY {} (postcode, LDZ) FROM STDIN").format(
                        Identifier(table_name))) as copy:
                for row in data:
                    post = [row[0] + row[1], row[2]]
                    try:
                        copy.write_row(post)
                    except psycopg.errors.UniqueViolation:
                        pass

    conn.close()


def setup_ldz_table(config):
    drop_table(config)
    create_ldz_table(config)
    insert_ldz(config)
