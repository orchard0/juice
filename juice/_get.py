import random
import time
import requests
from requests.auth import HTTPBasicAuth
import csv
from datetime import datetime, timedelta, UTC

from ._psql import (
    create_octopus_products_db,
    create_unit_rates_db,
    create_consumption_db,
    drop_table,
    insert_consumption,
    insert_octopus_energy_products,
    insert_unit_rates,
    count,
    create_calorific_value_db,
    query_missing_calorific,
    insert_calorific,
    insert_updates,
    query_updates,
)
from ._data import OCOTPUS_API_BASE_URL, PAGE_SIZE, UPDATE_INTERVAL

CONSUMPTION_URL = (
    OCOTPUS_API_BASE_URL
    + "/{energy_type}-meter-points/{mpan_or_mprn}/meters/{serial_number}/consumption"
)
TARIFFS_URL = (
    OCOTPUS_API_BASE_URL
    + "/products/{product_code}/{energy_type}-tariffs/{tariff_code}"
)


@staticmethod
def _get_octopus_products(psql_config, headers=None):
    table_name = "products_octopus_energy"

    if datetime.now(UTC) < query_updates(psql_config, table_name) + timedelta(
        hours=UPDATE_INTERVAL
    ):
        print("Skipping recently updated Octopus Products database.")
        return
    else:
        print("Updating Octopus Products database.")

    url = OCOTPUS_API_BASE_URL + "/products"

    r = requests.get(url, headers=headers)

    data = r.json()["results"]

    create_octopus_products_db(psql_config, table_name)
    insert_octopus_energy_products(psql_config, data, table_name)

    insert_updates(psql_config, table_name)

    pass


def _octopus_custom_products_download(psql_config, products, headers=None):
    print("Updating Octopus Products database")
    create_octopus_products_db(psql_config)
    url = "https://api.octopus.energy/v1/products/{product}/"

    for product in products:

        print(f"Getting {product}")

        product_url = url.format(product=product)

        r = requests.get(product_url, headers=headers)
        data = r.json()

        if len(data.keys()) == 1:
            print(f"{product} was not found.", r.status_code)
            continue

        insert_octopus_energy_products(psql_config, [data])

        time.sleep(random.random())


@staticmethod
def _get_calorific_values(
    psql_config, moved_in_at, LDZ, force_refresh=False, headers=None
):

    table_name = "calorific_values"
    update_name = table_name + "_" + LDZ

    if not force_refresh and datetime.now(UTC) < query_updates(
        psql_config, update_name
    ) + timedelta(hours=UPDATE_INTERVAL):
        print(f"Skipping recently updated calorific values for {LDZ} LDZ.")
        return

    PUB_ID_LOOKUP = {
        "Campbeltown": "PUBOBJ1660",
        "EA": "PUBOB4507",
        "EM": "PUBOB4508",
        "NE": "PUBOB4510",
        "NO": "PUBOB4509",
        "NT": "PUBOB4511",
        "NW": "PUBOB4512",
        "SC": "PUBOB4513",
        "SE": "PUBOB4514",
        "SO": "PUBOB4515",
        "SW": "PUBOB4516",
        "WM": "PUBOB4517",
        "WN": "PUBOB4518",
        "WS": "PUBOB4519",
        "Oban": "PUBOB4521",
        "Stornoway": "PUBOB4520",
        "Stranraer": "PUBOB4522",
        "Thurso": "PUBOBJ1661",
        "Wick": "PUBOBJ1662",
    }

    pub_id = PUB_ID_LOOKUP[LDZ]
    national_gas_api = "https://data.nationalgas.com/api/find-gas-data-download?applicableFor=Y&dateFrom={from_date}&dateTo={to_date}&dateType=GASDAY&latestFlag=Y&ids={pub_ids}&type=CSV"

    if force_refresh:
        drop_table(psql_config, table_name)

    create_calorific_value_db(psql_config)

    moved_in_at = datetime.fromisoformat(moved_in_at).strftime("%Y-%m-%d")

    earliest_date = query_missing_calorific(psql_config, moved_in_at, LDZ)

    tomorrow = datetime.now() + timedelta(days=1)
    to_date = datetime.strftime(tomorrow, "%Y-%m-%d")

    if earliest_date:
        from_date = earliest_date.strftime("%Y-%m-%d")
    else:
        week_ago = datetime.now() - timedelta(days=7)
        from_date = datetime.strftime(week_ago, "%Y-%m-%d")

    url = national_gas_api.format(from_date=from_date, to_date=to_date, pub_ids=pub_id)

    print(f"Getting calorific values for {LDZ} LDZ from {from_date}.")

    with requests.get(url, stream=True, headers=headers) as r:
        lines = (line.decode("utf-8") for line in r.iter_lines())
        data = list(csv.reader(lines))[1:]

    insert_calorific(psql_config, data)
    insert_updates(psql_config, update_name)


@staticmethod
def _get_consumption(
    psql_config,
    api_key,
    energy_type,
    mpan_or_mprn,
    serial_number,
    account_id,
    force_refresh=False,
    headers=None,
):

    table_name = account_id + "_" + mpan_or_mprn + "_" + serial_number

    if not force_refresh and datetime.now(UTC) < query_updates(
        psql_config, table_name
    ) + timedelta(hours=UPDATE_INTERVAL):
        print(
            f"Skipping recently updated consumption data for {serial_number} at {mpan_or_mprn} meter point for account {account_id}."
        )
        return
    else:
        print(
            f"Getting consumption data for {serial_number} at {mpan_or_mprn} meter point for account {account_id}."
        )

    if force_refresh:
        drop_table(psql_config, table_name)

    create_consumption_db(psql_config, table_name)

    url = (
        CONSUMPTION_URL.format(
            energy_type=energy_type,
            mpan_or_mprn=mpan_or_mprn,
            serial_number=serial_number,
        )
        + f"?page_size={PAGE_SIZE}"
    )

    while True:
        r = requests.get(url, auth=HTTPBasicAuth(api_key, ""), headers=headers)
        data = r.json()

        curr_count = count(psql_config, table_name)
        total_count = data["count"]
        results = data["results"]

        if total_count == curr_count:
            break

        insert_consumption(psql_config, results, table_name)

        if data["next"]:
            url = data["next"]
        else:
            break

        time.sleep(random.random())

    insert_updates(psql_config, table_name)


@staticmethod
def _get_tariffs(psql_config, tariff_code, force_refresh=False, headers=None):

    if "G-1R" in tariff_code:
        energy_type = "gas"
    else:
        energy_type = "electricity"

    for type in ["standard-unit-rates", "standing-charges"]:

        table_name = tariff_code + "_" + type.replace("-", "_")

        if not force_refresh and datetime.now(UTC) < query_updates(
            psql_config, table_name
        ) + timedelta(hours=UPDATE_INTERVAL):
            print(f"Skipping recently updated {tariff_code} {type.replace('-', ' ')}.")
            return
        else:
            print(f"Getting {tariff_code} {type.replace('-', ' ')}.")

        url = (
            TARIFFS_URL.format(
                energy_type=energy_type,
                product_code=tariff_code[5:-2],
                tariff_code=tariff_code,
            )
            + f"/{type}?page_size={PAGE_SIZE}"
        )

        if force_refresh:
            drop_table(psql_config, table_name)

        create_unit_rates_db(psql_config, table_name)

        while True:
            r = requests.get(url, headers=headers)
            data = r.json()

            try:
                curr_count = count(psql_config, table_name)
                total_count = data["count"]
                results = data["results"]
            except KeyError:
                print(f"There is no information available for {tariff_code}.")
                drop_table(psql_config, table_name)
                return

            if total_count == curr_count:
                break

            insert_unit_rates(psql_config, results, table_name)

            if data["next"]:
                url = data["next"]
            else:
                break

            time.sleep(random.random())

        insert_updates(psql_config, table_name)
