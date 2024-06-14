from ._psql import (
    create_updates_table,
    query_octopus_product_by_family_name,
    query_octopus_product_by_product_code,
    query_octopus_product_not_in_database
)
from ._get import octopus_custom_products_download


def update_database_with_product_code(self, product_codes):
    
    products  = query_octopus_product_not_in_database(self.psql_config, product_codes)
    if not products:
        print('All the products are already in the database.')
        return
    
    octopus_custom_products_download(self.psql_config, products, self.headers)


def update_by_tariff_code(self, tariff_codes, force_refresh=None):
    for entry in tariff_codes:
        self.get_tariffs(
            self.psql_config,
            entry["tariff_code"],
            force_refresh=force_refresh,
            headers=self.headers,
        )


def update_existing_products(self, force_refresh=None):

    tariff_codes = self.query_existing_products_tables(self.psql_config)
    self.update_by_tariff_code(tariff_codes, force_refresh)


def update_by_product_code(self, product_codes, force_refresh=None, energy_type=None):

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    if isinstance(product_codes, str):
        product_codes = [product_codes]

    tariff_codes = []
    for product_name in product_codes:
        tariff_codes.extend(
            query_octopus_product_by_product_code(
                self.psql_config, product_name, energy_type, self.GSP
            )
        )

    self.update_by_tariff_code(tariff_codes, force_refresh)


def update_by_product_family(self, product_families, force_refresh=None, energy_type=None):

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    if isinstance(product_families, str):
        product_families = [product_families]

    tariff_codes = []
    for product_name in product_families:
        tariff_codes.extend(
            query_octopus_product_by_family_name(
                self.psql_config, product_name, energy_type, self.GSP
            )
        )

    self.update_by_tariff_code(tariff_codes, force_refresh)

    


def update(self, force_refresh=False):

    print(f'Updating {self.ACCOUNT_DATA['number']}.')

    create_updates_table(self.psql_config)

    for consumption in self.CONSUMPTION:
        self.get_consumption(self.psql_config, self.API_KEY,
                                account_id=self.ACCOUNT_ID,
                                force_refresh=force_refresh,
                                headers=self.headers,
                                **consumption)

    for tariff in self.AGREEMENTS:
        self.get_tariffs(self.psql_config, tariff['tariff_code'], force_refresh=force_refresh, headers=self.headers)

    self.get_octopus_products(self.psql_config, self.headers)

    if self.calcs['gas']['consumption_dbs']:
        if not self.LDZ:
            raise ValueError('There were gas consumption databases found but no LDZ. Please add it to the Juice constructor.')
        for x in self.ACCOUNT_DATA['properties']:
            self.get_calorific_values(self.psql_config, x['moved_in_at'], self.LDZ, headers=self.headers)

    self.update_existing_products(force_refresh)

    print('Completed update.')
    print('='*15)
