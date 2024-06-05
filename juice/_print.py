from prettytable import PrettyTable
import pandas as pd
import numpy as np


def print_bill(self, from_date, to_date, energy_type=None):

    self.print_method('Bill', from_date, to_date, energy_type)


def print_method(self, name, from_date, to_date, energy_type=None):

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    method = self.search_method(energy_type, name)
    if not method:
        raise ValueError(
            f'The method "{name}" does not exist. Did you add it to the correct energy type and then call calculate?'
        )

    print_cost(method, from_date, to_date)


def print_cost(method, from_date, to_date):

    data = method['dataframe']

    cost_unit_rate = method['name'] + '_cost_unit_rate'
    cost_standing_charge = method['name'] + '_cost_standing_charge'

    def format_pound(item):
        return f"£{'{0:.2f}'.format(item)}"

    def format_days(days):
        if days == 1:
            return f"{days} day"
        else:
            return f"{days} days"

    from_date = pd.to_datetime(from_date).to_datetime64()
    to_date = pd.to_datetime(to_date).to_datetime64()

    from_date_converted = pd.to_datetime(from_date).tz_localize(
        'Europe/London').tz_convert('UTC').to_datetime64()
    to_date_converted = pd.to_datetime(to_date).tz_localize(
        'Europe/London').tz_convert('UTC').to_datetime64()

    if from_date and to_date:
        extracted_aware = data.loc[(data['from'] < to_date_converted)
                                   & (data['to'] > from_date_converted)]

        extracted_naive = data.loc[(data['from'] < to_date)
                                   & (data['to'] > from_date)]

    print(from_date, 'to', to_date)
    print('Dates extracted - total rows:', extracted_aware.shape[0])

    try:
        print(extracted_naive['calorific_value'].mean().round(1))
        print(extracted_naive['calorific_value'].mean().round(1))
    except KeyError:
        pass

    try:
        use = extracted_aware['consumption_kWh_rounded'].sum().round(1)
    except KeyError:
        use = extracted_aware['consumption_rounded'].sum().round(1)

    cost_rate = (extracted_aware[cost_unit_rate].sum() / 100)
    # standing charges and the number of days are worked out on the dataframe extracted using tz-naive dates
    days = (extracted_naive['to'].max() - extracted_naive['from'].min()).days
    cost_standing = (extracted_naive[cost_standing_charge].sum() / 100)
    subtotal = cost_rate + cost_standing
    vat = ((subtotal / 100) * 5).round(2)
    total = (subtotal + vat).round(2)

    table = PrettyTable([method['name'], 'Consumption', 'Cost'])
    table.add_row(["Total consumption", f"{use}kWh", format_pound(cost_rate)])
    table.add_row(
        ["Standing charge",
         format_days(days),
         format_pound(cost_standing)])
    table.add_row(["Subtotal", "", format_pound(subtotal)])
    table.add_row(["Vat at 5%", "", format_pound(vat)], divider=True)
    table.add_row(["Total", "", format_pound(total)])
    table.align = "l"
    table.align['Cost'] = "r"

    print(table)


def print_compare(self, from_date=None, to_date=None, energy_type=None):

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    data = self.calcs[energy_type]

    if from_date and to_date:
        from_date = pd.to_datetime(from_date).to_datetime64()
        to_date = pd.to_datetime(to_date).to_datetime64()

    names = []
    totals = []
    for method in data['methods']:
        try:
            datax = method['dataframe']

        except KeyError:
            raise ValueError(
                'The calculations for compare were not found. It probably means you need to use the run() method on the class before calling compare.'
            )

        name = method['name']
        names.append(name)

        if from_date and to_date:
            datax = datax.loc[(datax['from'] >= from_date)
                              & (datax['from'] < to_date)]
        total = datax[name + '_total'].sum() * np.float64(1.05)
        totals.append(total)

    totals_np = np.array(totals)
    names_np = np.array(names)

    out = pd.DataFrame(
        ((totals_np[:, None] - totals_np) / np.abs(total) * 100).round(1),
        index=names_np,
        columns=names_np)
    np.fill_diagonal(out.values, np.nan)

    totals_in = (totals_np / 100).round(2)
    totals_df = pd.DataFrame(totals_in, index=names, columns=['Total (£)'])

    table = pd.merge(out, totals_df, left_index=True, right_index=True)
    print(table)

    pass


def print_checks(self, energy_type=None):

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    data = self.calcs[energy_type]

    for method in data['methods']:
        print('Total final rows for', method['name'],
              method['dataframe'].shape[0])
        find_duplicates(method['dataframe'])


def find_duplicates(table):
    try:
        print(pd.concat(g for _, g in table.groupby("from") if len(g) > 1))
    except ValueError:
        print('No duplicates found.')

    pass
