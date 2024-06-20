import json
import pandas
from os import path
from pathlib import Path
import sys
import getpass
from ._data import (
    OCTOPUS_SMART_TARIFF_PRODUCT_CODES,
    OCTOPUS_SMART_TARIFF_FAMILIES,
    AGILE_OCTOPUS,
)
from datetime import datetime

ENERGY_TYPES = {"gas", "electricity"}


class Juice:


    from ._account import (
        _set_account_info,
        _read_account_json,
        _get_account_info,
        _parse_account_information,
    )
    from ._get import (
        _get_octopus_products,
        _get_consumption,
        _get_tariffs,
        _get_calorific_values,
    )
    from ._tariffs import (
        remove_method,
        add_bill,
        _add_consumption,
        add_method,
        add_method_by_product_family,
        add_method_by_product_code,
    )
    from ._bill import _run_config, calculate
    from ._print import print_bill, print_method, print_compare, print_checks
    from ._psql import query_existing_products_tables
    from ._database import (
        update_by_product_code,
        update_by_product_family,
        update_by_tariff_code,
        update_existing_products,
        update,
        update_products_database_by_product_code,
    )

    def __init__(
        self,
        API_KEY: str,
        ACCOUNT_ID: str,
        psql_config: dict | None = None,
        LDZ: str | None =None,
        energy_type: str | None =None,
        headers: dict | None =None,
    ) -> None:
        
        """
        Juice constructor prepares the account information to carry out calculations and comparisons.

        Examples:
            >>> account = Juice('api_key', 'A-W235SNT')

        Args:
            API_KEY: The api key for accessing the Octopus account.
            ACCOUNT_ID: The account id of the Octopus account.
            psql_config: A dictionary containing PostgresQL connection settings.
            LDZ: A string with the account's gas local distribution zone id. It's only need for accounts with gas energy and when a LDZ database has not been configured.
            energy_type: An energy type to be set for succeeding methods to use.
            headers: A dictionary containing settings to be passed on to Python Requests library when making network requests. 

        Returns:
            Juice constructor
        
        """

        self._create_working_dirs()

        if psql_config:
            self.psql_config = psql_config
        else:
            self.psql_config = {
                "user": getpass.getuser(),
                # 'password': 'password_string',
                "host": "127.0.0.1",
                "port": "5432",
                "dbname": "utilities",
                "autocommit": True,
            }

        self.API_KEY = API_KEY
        self.ACCOUNT_ID = ACCOUNT_ID.upper()
        self.ACCOUNT_DATA = self._set_account_info()
        self.CONSUMPTION, self.AGREEMENTS, self.GSP, queried_ldz = (
            self._parse_account_information(self.ACCOUNT_DATA)
        )

        self.ELEC_EARLIEST = self._retrive_earliest(self.AGREEMENTS, "electricity")
        self.GAS_EARLIEST = self._retrive_earliest(self.AGREEMENTS, "gas")

        if LDZ:
            self.LDZ = LDZ
        else:
            self.LDZ = queried_ldz

        self.energy_type = energy_type

        self.calcs = {
            "gas": {
                "methods": [],
            },
            "electricity": {
                "methods": [],
            },
        }

        self._add_consumption()

        self.headers = headers

        pass

    @property
    def data(self):

        """
        Return json formated calculations settings.

        Example:
            >>> account.data

        Args:
            None

        Returns:
            str
        
        """

        def handle(i):
            if type(i) == pandas.core.frame.DataFrame:
                return "PANDAS_DATAFRAME"
            else:
                return str(i)

        return json.dumps(self.calcs, default=handle)

    def set_energy(self, energy_type: str):

        """
        Set energy type for succeeding methods that rely on an optional energy_type parameter.

        Example:
            >>> account.set_energy('gas')

        Args:
            energy_type: The energy type to set.

        
        Returns:
            None
        """


        self._check_energy_type_input(energy_type)
        self.energy_type = energy_type

        if energy_type == "electricity":
            self.MOVED_IN_AT = self.ELEC_EARLIEST
        else:
            self.MOVED_IN_AT = self.GAS_EARLIEST
        pass

    @staticmethod
    def _retrive_earliest(agreements, energy_type):

        filtered = filter(lambda n: n["energy_type"] == energy_type, agreements)
        try:
            return sorted(
                filtered,
                key=lambda n: n["valid_from"],
            )[
                0
            ]["valid_from"]
        except:
            return None

    def _search_method(self, energy_type, name):

        method = list(
            filter(
                lambda n: n["name"].lower() == name.lower(),
                self.calcs[energy_type]["methods"],
            )
        )

        if len(method) == 0:
            return None
        else:
            return method[0]

    @staticmethod
    def _check_energy_type_input(energy_type):

        if energy_type not in ENERGY_TYPES:
            raise ValueError(
                f'energy_type must either be "gas" or "electricity". The function received: {energy_type}'
            )

        pass

    @staticmethod
    def _create_working_dirs():
        script_dir = path.dirname(sys.argv[0])
        accounts_dir = path.join(script_dir, "Accounts")
        Path(accounts_dir).mkdir(parents=True, exist_ok=True)
