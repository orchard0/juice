from ._psql import query_octopus_product_by_family_name, query_octopus_product_by_product_code

def remove_method(self, name: str, energy_type: str | None = None):

    """
    Remove a calculation method.

    Examples:
        >>> account.remove_tariff('Bill')

    Args:
        name: The name of the calculation method to remove.
        energy_type: The energy type from which to remove the calculation method.

    Returns:
        None
        
    """

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    data = self.calcs[energy_type]

    remove_indexes = [
        index for index, method in enumerate(data['methods'])
        if method['name'].lower() == name.lower()
    ]

    for index in remove_indexes:
        del data['methods'][index]
        print('Removed', name)


def add_method_by_product_family(self, family_name: str, energy_type: str | None =None):

    """
    Add a calculation method using an Octopus product family. e.g. "Agile Octopus". Only tariffs available in your local database will be added to the method.

    Examples:
        >>> account.add_method_by_product_family('Agile Octopus')


    Args:
        family_name: The name of the Octopus product family to add. e.g. "Flexible Octopus"
        energy_type: Set the energy type for this particular method call.

    Returns:
        None
        
    """

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    if isinstance(family_name, list):
        for dn in family_name:
            tariffs = query_octopus_product_by_family_name(self.psql_config, dn, energy_type, self.GSP)
            self.add_method(dn, tariffs, energy_type)
    elif isinstance(family_name, str):
        tariffs = query_octopus_product_by_family_name(self.psql_config, family_name, energy_type, self.GSP)
        self.add_method(family_name, tariffs, energy_type)
    pass

def add_method_by_product_code(self, product_code: str , energy_type: str | None =None):

    """
    Add a calculation method using an Octopus product code. e.g. "AGILE-23-12-06".

    Examples:
        >>> account.add_method_by_product_family('AGILE-23-12-06')


    Args:
        product_code: The Octopus product code to add. e.g. "SILVER-23-12-06"
        energy_type: Set the energy type for this particular method call.

    Returns:
        None
        
    """
        
    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    if isinstance(product_code, list):
        for dn in product_code:
            tariffs = query_octopus_product_by_product_code(self.psql_config, dn, energy_type, self.GSP)
            self.add_method(dn, tariffs, energy_type)
    elif isinstance(product_code, str):
        tariffs = query_octopus_product_by_product_code(self.psql_config, product_code, energy_type, self.GSP)
        self.add_method(product_code, tariffs, energy_type)
    pass


def add_bill(self, energy_type=None):

    """
    Add the account bill to the calculation methods.

    Examples:
        >>> account.add_bill()


    Args:
        energy_type: Set the energy type for which the bill should be calculated.

    Returns:
        None
        
    """

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    agreements = list(
        filter(lambda n: n['energy_type'] == energy_type, self.AGREEMENTS))

    if len(agreements) == 0:
        raise ValueError(
            f"There are were no agreements for {energy_type} found in the account information."
        )

    self.add_method('Bill', agreements, energy_type, replace=True)
    pass

def add_method(self, name: str, tariff_agreements: list, energy_type: str | None = None, replace: bool = False):

    """
    Add a calculation method using a custom tariff list.

    Examples:
        >>> custom_list =  [
          {
            "tariff_code": "E-1R-VAR-22-10-01-C",
            "valid_from": "2023-03-17 00:00:00+00:00",
            "valid_to": "2023-03-18 00:00:00+00:00"
          },
          {
            "tariff_code": "E-1R-AGILE-FLEX-22-11-25-C",
            "valid_from": "2023-03-18 00:00:00+00:00",
            "valid_to": "2024-02-15 00:00:00+00:00"
          },
           {
            "tariff_code": "E-1R-INTELLI-VAR-22-10-14-C",
            "valid_from": "2024-02-15 00:00:00+00:00",
            "valid_to": None
          }
          ]
        >>> account.add_method('Custom tariff', custom_list)


    Args:
        name: The unique name for this calculation method.
        tariff_agreements: A custom list of tariff codes with valid from and valid to dates.
        energy_type: The energy type for which to add this calculation method.
        replace: Whether to replace an existing calculation method with the same name.

    Returns:
        None
        
    """

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    data = self.calcs[energy_type]

    searched = self.search_method(energy_type, name)
    if searched and replace:
        self.remove_tariff(name, energy_type)
    elif searched and not replace:
        raise ValueError(f'{name} method already exist. Either remove it first or use the replace parameter.')

    def dates_for_method(agreements):

        x = sorted(agreements, key=lambda d: d['valid_from'])

        from_date = x[0]['valid_from']
        to_date = x[-1]['valid_to']

        return from_date, to_date

    method_from_date, method_to_date = dates_for_method(tariff_agreements)

    method = {
        'name': name,
        'agreements': tariff_agreements,
        'from_date': method_from_date,
        'to_date': method_to_date,
        'cost_types': {
            '_standard_unit_rates': [],
            '_standing_charges': [],
        }        
    }
    
    data['methods'].append(method)

    pass


def _add_consumption(self):
    for energy_type in ['gas', 'electricity']:

        data = self.calcs[energy_type]

        consumption_dbs = list(
            filter(lambda n: n['energy_type'] == energy_type, self.CONSUMPTION))

        #TODO should the user be made aware?
        # if len(consumption_dbs) == 0:
        #     raise ValueError(
        #         f"There are no {energy_type} consumption databases found in the account information."
        #     )

        data['consumption_dbs'] = [f'{self.ACCOUNT_ID}_{x['mpan_or_mprn']}_{x['serial_number']}' for x in consumption_dbs]

        pass
