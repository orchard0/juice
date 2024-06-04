from datetime import datetime, timedelta
import json
import requests
from requests.auth import HTTPBasicAuth

from ._psql import query_ldz
from ._data import OCOTPUS_API_BASE_URL


def set_account_info(self):
    data = self.read_account_json(self.ACCOUNT_ID)
    if not data:
        data = self.get_account_info(self.API_KEY, self.ACCOUNT_ID)

    return data


@staticmethod
def read_account_json(ACCOUNT_ID):

    try:
        with open(f'./Accounts/{ACCOUNT_ID}.json', 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        return None

    updated_last = datetime.strptime(data['updated'], '%Y-%m-%d %H:%M:%S.%f')
    if updated_last + timedelta(days=1) < datetime.now():
        return None

    return data


@staticmethod
def get_account_info(API_KEY, ACCOUNT_ID):
    ACCOUNT_URL = OCOTPUS_API_BASE_URL + f'/accounts/{ACCOUNT_ID}'

    response = requests.get(ACCOUNT_URL, auth=HTTPBasicAuth(API_KEY, ''))

    data = response.json()
    if data == {'detail': 'Not found.'}:
        raise ValueError(f'The account {ACCOUNT_ID} was not found!')

    data['updated'] = datetime.now()

    for property in data['properties']:
        if property['gas_meter_points']:
            property['LDZ'] = query_ldz(property['postcode'].replace(' ', ''))

    with open(f'./Accounts/{ACCOUNT_ID}.json', 'w') as file:
        file.write(json.dumps(data, default=str))

    return data


@staticmethod
def parse_account_information(data):
    ''''
    Return all meters and agreements found in account data.
    '''

    meter_data = []
    agreements_data = []
    ldz = None
    for property in data['properties']:

        for energy_type in ['electricity', 'gas']:
            for meter_point in property[energy_type + '_meter_points']:
                if energy_type == 'electricity':
                    mpan_or_mprn = meter_point['mpan']
                else:
                    mpan_or_mprn = meter_point['mprn']
                    try:
                        ldz = property['LDZ']
                    except KeyError:
                        ldz = query_ldz(property['postcode'].replace(' ', ''))

                for meter in meter_point['meters']:
                    serial_number = meter['serial_number']
                    meter = {
                        'mpan_or_mprn': mpan_or_mprn,
                        'serial_number': serial_number,
                        'energy_type': energy_type,
                    }
                    meter_data.append(meter)

                for agreement in meter_point['agreements']:
                    agreement['energy_type'] = energy_type
                    agreements_data.append(agreement)

    gsp = agreements_data[0]['tariff_code'][-1]
    return meter_data, agreements_data, gsp, ldz
