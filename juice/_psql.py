from datetime import datetime
import psycopg
from psycopg.sql import SQL, Identifier, Literal, Composed
import re


def create_updates_table(psql_config, table_name="updates"):
    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    try:
        curr.execute(
            SQL(
                """
            CREATE TABLE IF NOT EXISTS {} (
                id serial PRIMARY KEY,
                name varchar unique not null,
                updated timestamptz not null
             )
            """
            ).format(Identifier(table_name))
        )

    except psycopg.errors.DuplicateTable:
        pass

    conn.close()
    pass


def insert_updates(psql_config, updated_table_name, table_name="updates"):
    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    try:
        curr.execute(
            SQL(
                """
            INSERT INTO {} (name, updated) VALUES ({}, CURRENT_TIMESTAMP) \
                ON CONFLICT (name) DO UPDATE \
                SET updated = EXCLUDED.updated 
            """
            ).format(Identifier(table_name), Literal(updated_table_name))
        )

    except psycopg.errors.DuplicateTable:
        pass

    conn.close()
    pass


def query_updates(psql_config, updated_table_name, table_name="updates"):
    query = SQL("select updated from {} where name = {} limit 1").format(
        Identifier(table_name), Literal(updated_table_name)
    )
    result = retrive(psql_config, query)
    try:
        r = result[0][0]
    except IndexError:
        r = datetime.datetime(1900, 1, 1, 0, 0, 0, 0, datetime.UTC)
    return r


def create_octopus_products_db(psql_config, table_name="products_octopus_energy"):
    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    try:
        curr.execute(
            SQL(
                """
            CREATE TABLE {} (
                id serial PRIMARY KEY,
                code varchar unique not null,
                full_name varchar not null,
                display_name varchar not null,
                description varchar not null,
                is_variable bool not null,
                is_green bool not null,
                is_tracker bool not null,
                is_prepay bool not null,
                is_business bool not null,
                is_restricted bool not null,
                term smallint,
                available_from timestamptz not null,
                available_to timestamptz,
                added timestamptz default CURRENT_TIMESTAMP,
                updated timestamptz,
                brand varchar not null
             )
            """
            ).format(Identifier(table_name))
        )

    except psycopg.errors.DuplicateTable:
        pass

    conn.close()
    pass


def insert_octopus_energy_products(
    psql_config, data, table_name="products_octopus_energy"
):
    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    for row in data:
        insert_query = SQL(
            "INSERT INTO {} (code, full_name, display_name, description, is_variable, is_green, is_tracker, is_prepay, is_business, is_restricted, term, available_from, available_to, brand) \
                VALUES (%(code)s, %(full_name)s, %(display_name)s, %(description)s, %(is_variable)s, %(is_green)s, %(is_tracker)s, %(is_prepay)s, %(is_business)s, %(is_restricted)s, %(term)s, %(available_from)s, %(available_to)s, %(brand)s)\
                    ON CONFLICT (code) DO UPDATE \
                        SET code = EXCLUDED.code, \
                            full_name = EXCLUDED.full_name, \
                            display_name = EXCLUDED.display_name, \
                            description = EXCLUDED.description, \
                            is_variable = EXCLUDED.is_variable, \
                            is_green = EXCLUDED.is_green, \
                            is_tracker = EXCLUDED.is_tracker, \
                            is_prepay = EXCLUDED.is_prepay, \
                            is_business = EXCLUDED.is_business, \
                            is_restricted = EXCLUDED.is_restricted, \
                            term = EXCLUDED.term, \
                            available_from = EXCLUDED.available_from, \
                            available_to = EXCLUDED.available_to, \
                            updated = CURRENT_TIMESTAMP, \
                            brand = EXCLUDED.brand"
        ).format(Identifier(table_name))
        try:
            curr.execute(insert_query, row)
        except psycopg.errors.UniqueViolation:
            raise

    conn.close()


def query_tariff_family(psql_config, display_name, brand="OCTOPUS_ENERGY"):
    query = SQL(
        "select code, full_name, display_name from products_octopus_energy \
            where display_name = {} and brand = {}"
    ).format(Literal(display_name), Literal(brand))

    result = retrive(psql_config, query)

    x = sorted(result, key=lambda d: d[1])

    return x


def query_octopus_product_not_in_database(psql_config, products_list):

    s = Composed(SQL("({})").format(Literal(n)) for n in products_list)

    query = SQL(
        "values {} except select distinct(code) from products_octopus_energy"
    ).format(s.join(", "))

    return [x[0] for x in retrive(psql_config, query)]


def query_octopus_product_by_product_code(psql_config, display_name, energy_type, gsp):
    query = SQL(
        "select code, full_name, display_name, available_from, available_to from products_octopus_energy \
            where code = {} and brand = 'OCTOPUS_ENERGY'"
    ).format(Literal(display_name))

    return prep_octopus_results(psql_config, query, energy_type, gsp)


def query_octopus_product_by_family_name(psql_config, display_name, energy_type, gsp):
    query = SQL(
        "select code, full_name, display_name, available_from, available_to from products_octopus_energy \
            where display_name = {} and brand = 'OCTOPUS_ENERGY'"
    ).format(Literal(display_name))

    return prep_octopus_results(psql_config, query, energy_type, gsp)


def prep_octopus_results(psql_config, query, energy_type, gsp):
    result = retrive(psql_config, query)

    x = sorted(result, key=lambda d: d[3])

    if energy_type == "electricity":
        prefix = "E-1R-"
    else:
        prefix = "G-1R-"

    tariff_list = []
    for index, item in enumerate(x):

        if len(x) == index + 1:
            valid_to = item[4]
        else:
            valid_to = x[index + 1][3]

        tariff_list.append(
            {
                "name": item[2],
                "tariff": item[0],
                "tariff_code": prefix + item[0] + "-" + gsp,
                "valid_from": item[3],
                "valid_to": valid_to,
            }
        )

    return tariff_list


def query_octopus_unique_tariff_family(
    psql_config, brand="OCTOPUS_ENERGY", table_name="products_octopus_energy"
):
    query = SQL(
        "select DISTINCT(display_name) from {} where brand = {} order by display_name"
    ).format(Identifier(table_name), Literal(brand))

    response = retrive(psql_config, query)
    return [name[0] for name in response]


@staticmethod
def query_existing_products_tables(psql_config):
    query = SQL(
        "SELECT tablename FROM pg_catalog.pg_tables where tablename like '%rates' or tablename like '%charges'"
    )
    result = retrive(psql_config, query)
    try:
        r = [
            table[0]
            .replace("_standing_charges", "")
            .replace("_standard_unit_rates", "")
            for table in result
        ]
        r = set(r)
        r = [{"tariff_code": code} for code in r]
    except IndexError:
        r = None
    return r


def create_calorific_value_db(psql_config, table_name="calorific_values"):

    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    try:
        curr.execute(
            SQL(
                """
            CREATE TABLE {} (
                id serial PRIMARY KEY,
                applicable_date timestamptz unique not null,
                exit_zone varchar not null,
                calorific_value numeric not null,
                unique (applicable_date, exit_zone)
             )
            """
            ).format(Identifier(table_name))
        )

    except psycopg.errors.DuplicateTable:
        pass

    conn.close()


def insert_calorific(psql_config, results, table_name="calorific_values"):

    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    for row in results:
        # extracts the LDZ code from the string i.e. 'NW' and replaces it back into the row
        row[2] = re.findall(r"(?:Calorific Value, LDZ\()(\w+)[\)]", row[2])[0]
        insert_query = SQL(
            "INSERT INTO {} (applicable_date, exit_zone, calorific_value) \
                VALUES (%s, %s, %s)\
                    ON CONFLICT (applicable_date, exit_zone) DO UPDATE \
                        SET calorific_value = EXCLUDED.calorific_value"
        ).format(Identifier(table_name))
        try:
            curr.execute(insert_query, row[1:4])
        except psycopg.errors.UniqueViolation:
            raise

    conn.close()


def query_missing_calorific(psql_config, date, ldz, table_name="calorific_values"):

    query = SQL(
        "SELECT * FROM generate_series({}, CURRENT_DATE, interval '1 day') AS dates WHERE dates NOT IN (SELECT applicable_date FROM {} where exit_zone = {}) LIMIT 1"
    ).format(Literal(date), Identifier(table_name), Literal(ldz))

    try:
        return retrive(psql_config, query)[0][0]
    except IndexError:
        return None
    except psycopg.errors.UndefinedTable:
        return date


def create_consumption_db(psql_config, table_name="consumption"):

    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    try:
        curr.execute(
            SQL(
                """
            CREATE TABLE {} (
                id serial PRIMARY KEY,
                consumption numeric not null,
                interval_start timestamptz not null,
                interval_end timestamptz not null,
                unique (interval_start, interval_end)
                )"""
            ).format(Identifier(table_name))
        )

        curr.execute(
            SQL(
                "create index on {} using gist (tstzrange(interval_start,interval_end))"
            ).format(Identifier(table_name))
        )

    except psycopg.errors.DuplicateTable:
        pass

    conn.close()


def insert_consumption(psql_config, results, table_name="consumption"):

    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    for item in results:
        insert_query = SQL(
            "INSERT INTO {} (consumption, interval_start, interval_end) \
                VALUES (%(consumption)s, %(interval_start)s, %(interval_end)s)\
                    ON CONFLICT (interval_start, interval_end) DO UPDATE \
                        SET consumption = EXCLUDED.consumption"
        ).format(Identifier(table_name))
        try:
            curr.execute(insert_query, item)
        except psycopg.errors.UniqueViolation:
            raise

    conn.close()


def create_unit_rates_db(psql_config, table_name="unit_rates"):

    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()
    try:
        curr.execute(
            SQL(
                """
                CREATE TABLE {} (
                    id serial PRIMARY KEY,
                    value_inc_vat numeric not null,
                    value_exc_vat numeric not null,
                    valid_from timestamptz,
                    valid_to timestamptz,
                    payment_method char(16),
                    unique nulls not distinct (valid_from, payment_method)
                )"""
            ).format(Identifier(table_name))
        )

        curr.execute(
            SQL(
                "create index on {} using gist (tstzrange(valid_from,valid_to))"
            ).format(Identifier(table_name))
        )

    except psycopg.errors.DuplicateTable:
        pass

    conn.close()


def insert_unit_rates(psql_config, results, table_name="unit_rates"):
    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    for item in results:
        insert_query = SQL(
            "INSERT INTO {} (value_inc_vat, value_exc_vat, valid_from, valid_to, payment_method) \
                VALUES (%(value_inc_vat)s, %(value_exc_vat)s, %(valid_from)s, %(valid_to)s, %(payment_method)s)\
                    ON CONFLICT (valid_from, payment_method) DO UPDATE \
                        SET value_inc_vat = EXCLUDED.value_inc_vat, \
                            value_exc_vat = EXCLUDED.value_exc_vat, \
                                    valid_to = EXCLUDED.valid_to"
        ).format(Identifier(table_name))

        try:
            curr.execute(insert_query, item)
        except psycopg.errors.UniqueViolation:
            raise

    conn.close()


def drop_table(psql_config, table_name):

    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()
    curr.execute(SQL("drop table if exists {}").format(Identifier(table_name)))

    conn.close()


def count(psql_config, table_name):
    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()
    query = SQL("select count(id) from {}").format(Identifier(table_name))
    curr.execute(query)
    result = curr.fetchone()
    conn.close()
    return result[0]


def retrive(psql_config, query, params=None):

    conn = psycopg.connect(**psql_config)
    curr = conn.cursor()

    try:
        curr.execute(query, params)
        # print(query.as_string(curr))
    except psycopg.errors.UndefinedTable:
        raise ValueError

    result = curr.fetchall()
    conn.close()
    return result


def retrive_consumption(psql_config, table_name, from_date, to_date):

    query = SQL(
        "select consumption, interval_start, interval_end from {} where interval_start >= {} and interval_start < {}"  # (interval_start between {} and {})
    ).format(Identifier(table_name), from_date, to_date)

    return retrive(psql_config, query)


def retrive_unit_rates(psql_config, table_name, payment_method, from_date, to_date):

    query = SQL(
        "select value_exc_vat, valid_from, valid_to from {} where (payment_method is NULL or payment_method = {}) and (valid_from < {} and (valid_to > {} or valid_to is null))"
    ).format(Identifier(table_name), payment_method, to_date, from_date)

    return retrive(psql_config, query)


def query_calorific_values(psql_config, table_name, exit_zone, from_date, to_date):

    query = SQL(
        "select calorific_value as value_exc_vat, applicable_date as valid_from, applicable_date + interval '1' day as valid_to from {} where exit_zone = {} and applicable_date >= {} and applicable_date < {}"
    ).format(
        Identifier(table_name),
        exit_zone,
        from_date,
        to_date,
    )

    return retrive(psql_config, query)


def query_ldz(psql_config, postcode, table_name="LDZ"):
    query = SQL("select ldz from {} where postcode = {} limit 1").format(
        Identifier(table_name), Literal(postcode)
    )
    result = retrive(psql_config, query)
    try:
        r = result[0][0]
    except IndexError:
        r = None
    return r
