from ._psql import (
    create_updates_table,
    query_octopus_product_by_family_name,
    query_octopus_product_by_product_code,
    query_octopus_product_not_in_database
)
from ._get import _octopus_custom_products_download


def update_products_database_by_product_code(self, product_codes: list | str):

    """
    Add product information to local Octopus products database.

    Examples:
        >>> product_codes = ['AGILE-FLEX-22-11-25', 'SILVER-FLEX-22-11-25']
        >>> account.update_products_database_by_product_code(product_codes)
        OR
        >>> account.update_products_database_by_product_code('AGILE-FLEX-22-11-25')


    Args:
        product_codes: A list of Octopus products to add to the products database.

    Returns:
        None
        
    """

    if isinstance(product_codes, str):
        product_codes = [product_codes]

    products  = query_octopus_product_not_in_database(self.psql_config, product_codes)
    if not products:
        print('All the products are already in the database.')
        return
    
    _octopus_custom_products_download(self.psql_config, products, self.headers)


def update_by_tariff_code(self, tariff_codes: list, force_refresh: bool | None = None):

    """
    Update/create databases of supplied Octopus tariff codes.

    Examples:
        >>> tariff_codes = ['E-1R-AGILE-FLEX-22-11-25-C', 'E-1R-INTELLI-VAR-22-10-14-C']
        >>> account.update_by_product_code(tariff_codes)
        OR
        >>> account.update_by_product_code('E-1R-AGILE-FLEX-22-11-25-C')

    Args:
        tariff_codes: A list of Octopus tariff codes to update.
        force_refresh: Whether to delete and recreate the databases.

    Returns:
        None
        
    """
    if isinstance(tariff_codes, str):
        tariff_codes = [tariff_codes]

    for entry in tariff_codes:
        self._get_tariffs(
            self.psql_config,
            entry["tariff_code"],
            force_refresh=force_refresh,
            headers=self.headers,
        )


def update_existing_products(self, force_refresh: bool | None =None):

    """
    Update existing Octopus tariff databases.

    Examples:
        >>> account.update_existing_products()

    Args:
        force_refresh: Whether to delete and recreate the databases.

    Returns:
        None
        
    """


    tariff_codes = self.query_existing_products_tables(self.psql_config)
    self.update_by_tariff_code(tariff_codes, force_refresh)


def update_by_product_code(self, product_codes: list | str, force_refresh: bool | None = None, energy_type: str | None = None):

    """
    Update/create databases of supplied Octopus product codes.

    Examples:
        >>> product_codes = ['Agile Octopus', 'Flexible Octopus']
        >>> account.update_by_product_code(product_codes)
        OR
        >>> account.update_by_product_code('Agile Octopus')


    Args:
        product_codes: A list of Octopus product codes to update.
        force_refresh: Whether to delete and recreate the databases.
        energy_type: Update for the specified energy type.

    Returns:
        None
        
    """
        
    if energy_type is None:
        energy_type = self.energy_type
    self._check_energy_type_input(energy_type)

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


def update_by_product_family(self, product_families: list | str, force_refresh: bool | None = None, energy_type: str | None = None):

    """
    Update/create tariff databases of supplied Octopus product families. Only tariffs available in your local Octopus products database will be updated.

    Examples:
        >>> product_families = ['Agile Octopus', 'Flexible Octopus']
        >>> account.update_by_product_family(product_families)
        OR 
        >>> account.update_by_product_family('Flexible Octopus')


    Args:
        product_families: A list of Octopus product families to update.
        force_refresh: Whether to delete and recreate the databases.
        energy_type: Update for the specified energy type.

    Returns:
        None
        
    """


    if energy_type is None:
        energy_type = self.energy_type
    self._check_energy_type_input(energy_type)

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

    


def update(self, force_refresh: bool | None = False):

    """
    Update/create consumption, account tariffs, Octopus products and existing tariff databases.

    Examples:
        >>> account.update()

    Args:
        force_refresh: Whether to delete and recreate the databases.

    Returns:
        None
        
    """

    print(f'Updating {self.ACCOUNT_DATA['number']}.')

    create_updates_table(self.psql_config)

    for consumption in self.CONSUMPTION:
        self._get_consumption(self.psql_config, self.API_KEY,
                                account_id=self.ACCOUNT_ID,
                                force_refresh=force_refresh,
                                headers=self.headers,
                                **consumption)

    for tariff in self.AGREEMENTS:
        self._get_tariffs(self.psql_config, tariff['tariff_code'], force_refresh=force_refresh, headers=self.headers)

    self._get_octopus_products(self.psql_config, self.headers)

    if self.calcs['gas']['consumption_dbs']:
        if not self.LDZ:
            raise ValueError('There were gas consumption databases found but no LDZ. Please add it to the Juice constructor.')
        for x in self.ACCOUNT_DATA['properties']:
            self._get_calorific_values(self.psql_config, x['moved_in_at'], self.LDZ, headers=self.headers)

    self.update_existing_products(force_refresh)

    print('Completed update.')
    print('='*15)
