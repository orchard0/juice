from ._psql import query_octopus_tariff_by_family, query_octopus_tariff_by_product_code

def remove_tariff(self, energy_type, name):

    data = self.calcs[energy_type]

    remove_indexes = [
        index for index, method in enumerate(data['methods'])
        if method['name'].lower() == name.lower()
    ]

    for index in remove_indexes:
        del data['methods'][index]
        print('removed', name)


def add_method_by_tariff_family(self, display_name, energy_type=None):

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    if isinstance(display_name, list):
        for dn in display_name:
            tariffs = query_octopus_tariff_by_family(self.psql_config, dn, energy_type, self.GSP)
            self.add_method(energy_type, dn, tariffs)
    elif isinstance(display_name, str):
        tariffs = query_octopus_tariff_by_family(self.psql_config, display_name, energy_type, self.GSP)
        self.add_method(energy_type, display_name, tariffs)
    pass

def add_method_by_product_code(self, product_code, energy_type=None):

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    if isinstance(product_code, list):
        for dn in product_code:
            tariffs = query_octopus_tariff_by_product_code(self.psql_config, dn, energy_type, self.GSP)
            self.add_method(energy_type, dn, tariffs)
    elif isinstance(product_code, str):
        tariffs = query_octopus_tariff_by_product_code(self.psql_config, product_code, energy_type, self.GSP)
        self.add_method(energy_type, product_code, tariffs)
    pass


def add_bill(self, energy_type=None):

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    agreements = list(
        filter(lambda n: n['energy_type'] == energy_type, self.AGREEMENTS))

    if len(agreements) == 0:
        raise ValueError(
            f"There are were no agreements for {energy_type} found in the account information."
        )

    self.add_method(energy_type, 'Bill', agreements, replace=True)
    pass

def add_method(self, energy_type, name, agreements, replace=False):

    data = self.calcs[energy_type]

    searched = self.search_method(energy_type, name)
    if searched and replace:
        self.remove_tariff(energy_type, name)
    elif searched and not replace:
        raise ValueError(f'{name} method already exist. Either remove it first or use the replace parameter.')

    def dates_for_method(agreements):

        x = sorted(agreements, key=lambda d: d['valid_from'])

        from_date = x[0]['valid_from']
        to_date = x[-1]['valid_to']

        return from_date, to_date

    method_from_date, method_to_date = dates_for_method(agreements)

    method = {
        'name': name,
        'agreements': agreements,
        'from_date': method_from_date,
        'to_date': method_to_date,
        'cost_types': {
            '_standard_unit_rates': [],
            '_standing_charges': [],
        }        
    }
    
    data['methods'].append(method)

    pass


def add_consumption(self):
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
