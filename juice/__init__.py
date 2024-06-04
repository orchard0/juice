import json
import pandas
from os import path
from pathlib import Path
import sys
import getpass
from ._psql import create_updates_table

ENERGY_TYPES = {'gas', 'electricity'}


class Juice:

    from ._account import set_account_info, read_account_json, get_account_info, parse_account_information
    from ._get import get_octopus_products, get_consumption, get_tariffs, get_calorific_values
    from ._tariffs import remove_tariff, add_bill, add_consumption, add_method, add_method_by_tariff_family
    from ._bill import run_config, calculate
    from ._print import print_bill, print_method, print_compare, print_checks

    def __init__(self, API_KEY, ACCOUNT_ID, psql_config=None, LDZ=None, energy_type=None, headers=None) -> None:
        self.create_working_dirs()

        if psql_config:
            self.psql_config = psql_config
        else:
            self.psql_config = {
            'user': getpass.getuser(),
            # 'password': 'password_string',
            'host': '127.0.0.1',
            'port': '5432',
            'dbname': 'utilities',
            'autocommit': True,
        }


        self.API_KEY = API_KEY
        self.ACCOUNT_ID = ACCOUNT_ID.upper()
        self.ACCOUNT_DATA = self.set_account_info()
        self.CONSUMPTION, self.AGREEMENTS, self.GSP, queried_ldz = self.parse_account_information(
            self.ACCOUNT_DATA)
        
        if LDZ:
            self.LDZ = LDZ
        else:
             self.LDZ = queried_ldz

        self.energy_type = energy_type

        self.calcs = {
            'gas': {
                'consumption_dbs': None,
                'methods': [],
                '_calorific_values': [],
                'energy_type': 'gas'
            },
            'electricity': {
                'consumption_dbs': None,
                'methods': [],
                '_calorific_values': [],
                'energy_type': 'electricity'
            }
        }

        self.add_consumption()

        self.headers = headers

        pass


    @property
    def data(self):

        def handle(i):
            if type(i) == pandas.core.frame.DataFrame:
                return 'PANDAS_DATAFRAME'
            else:
                return str(i)

        return json.dumps(self.calcs, default=handle)

    def set_energy(self, energy_type):
        self.check_energy_type_input(energy_type)
        self.energy_type = energy_type
        pass

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
            res = {key: tariff[key] for key in {'energy_type', 'tariff_code'}}
            self.get_tariffs(self.psql_config, force_refresh=force_refresh, headers=self.headers, **res)

        self.get_octopus_products(self.psql_config, self.headers)

        if self.calcs['gas']['consumption_dbs']:
            if not self.LDZ:
                raise ValueError('There were gas consumption databases found but no LDZ. Please add it to the Juice constructor.')
            for x in self.ACCOUNT_DATA['properties']:
                self.get_calorific_values(self.psql_config, x['moved_in_at'], self.LDZ, headers=self.headers)

        print('Completed update.')
        print('='*15)


    def search_method(self, energy_type, name):

        method = list(
            filter(lambda n: n['name'].lower() == name.lower(),
                   self.calcs[energy_type]['methods']))

        if len(method) == 0:
            return None
        else:
            return method[0]

    @staticmethod
    def check_energy_type_input(energy_type):

        if energy_type not in ENERGY_TYPES:
            raise ValueError(
                f'energy_type must either be "gas" or "electricity". The function received: {energy_type}'
            )

        pass

    @staticmethod
    def create_working_dirs():
        script_dir = path.dirname(sys.argv[0])
        accounts_dir = path.join(script_dir, 'Accounts')
        Path(accounts_dir).mkdir(parents=True, exist_ok=True)