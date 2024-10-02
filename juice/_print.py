from prettytable import PrettyTable
import pandas as pd
import numpy as np
from ._utils import _parse_date, _format_date
from datetime import datetime


def print_bill(self, from_date: str | datetime = None, to_date: str | datetime = None, energy_type: str | None = None):

    """
    Print the bill for the account.

    Examples:
        >>> account.print_bill()
        >>> account.print_bill('2023-01-15', '2024-04-16')


    Args:
        from_date: The date from which to print the costs.
        to_date: The date to which to print the costs.
        energy_type: The energy type to use in the bill.

    Returns:
        None
        
    """

    self.print_method("Bill", from_date, to_date, energy_type)


def print_method(self, name, from_date: str | datetime = None, to_date: str | datetime = None, energy_type: str | None = None):

    """
    Print the bill for the specified calculation method.

    Examples:
        >>> account.print_method('Agile Octopus')
        >>> account.print_method('Flexible Octopus', '2023-01-15', '2024-04-16')


    Args:
        from_date: The date from which to print the costs.
        to_date: The date to which to print the costs.
        energy_type: The energy type to use in the bill.

    Returns:
        None
        
    """


    if energy_type is None:
        energy_type = self.energy_type
    self._check_energy_type_input(energy_type)


    if from_date:
        from_date = _parse_date(from_date)
    else:
        from_date = self.MOVED_IN_AT

    if to_date:
        to_date = _parse_date(to_date, 1)
    else:
        to_date = _parse_date(add=-1)

    data = self.calcs[energy_type]
    calcs_from_date = data["from_date"]

    if from_date < data["from_date"]:
        raise ValueError(
            f"{_format_date(from_date)} is earlier than the from_date used in the calculations {_format_date(calcs_from_date)}."
        )

    method = self._search_method(energy_type, name)
    if not method:
        raise ValueError(
            f'The method "{name}" does not exist. Did you add it to the correct energy type and then call calculate?'
        )

    _print_cost(method, from_date, to_date)

@staticmethod
def _print_cost(method, from_date, to_date):

    energy_type = None

    data = method["dataframe"]

    name = method["name"]
    cost_unit_rate = name + "_cost_unit_rate"
    cost_standing_charge = name + "_cost_standing_charge"

    def format_pound(item):
        return f"£{'{0:.2f}'.format(item)}"

    def format_days(days):
        if days == 1:
            return f"{days} day"
        else:
            return f"{days} days"

    from_date = pd.to_datetime(from_date).to_datetime64()
    to_date = pd.to_datetime(to_date).to_datetime64()

    from_date_converted = (
        pd.to_datetime(from_date)
        .tz_localize("Europe/London")
        .tz_convert("UTC")
        .to_datetime64()
    )
    to_date_converted = (
        pd.to_datetime(to_date)
        .tz_localize("Europe/London")
        .tz_convert("UTC")
        .to_datetime64()
    )

    if from_date and to_date:
        extracted_aware = data.loc[
            (data["from"] < to_date_converted) & (data["to"] > from_date_converted)
        ]

        extracted_naive = data.loc[(data["from"] < to_date) & (data["to"] > from_date)]

    print(name, "from", from_date, "to", to_date)
    print("Dates extracted - total rows:", extracted_aware.shape[0])

    try:
        print(extracted_naive["calorific_value"].mean().round(1))
        use_gas_units = extracted_aware["consumption_units"].sum().round(1)
        energy_type = "gas"
    except KeyError:
        pass

    try:
        use = extracted_aware["consumption_kWh_rounded"].sum().round(1)
    except KeyError:
        use = extracted_aware["consumption_rounded"].sum().round(1)

    cost_rate = extracted_aware[cost_unit_rate].sum() / 100
    # standing charges and the number of days are worked out on the dataframe extracted using tz-naive dates
    days = (extracted_naive["to"].max() - extracted_naive["from"].min()).days
    cost_standing = extracted_naive[cost_standing_charge].sum() / 100
    subtotal = cost_rate + cost_standing
    vat = ((subtotal / 100) * 5).round(2)
    total = (subtotal + vat).round(2)

    table = PrettyTable([name, "Consumption", "Cost"])
    if energy_type == "gas":
        consumption_row = f"{use} kWh ({use_gas_units} m³)"
    else:
        consumption_row = f"{use}kWh"

    table.add_row(["Total consumption", consumption_row, format_pound(cost_rate)])
    table.add_row(["Standing charge", format_days(days), format_pound(cost_standing)])
    table.add_row(["Subtotal", "", format_pound(subtotal)])
    table.add_row(["Vat at 5%", "", format_pound(vat)], divider=True)
    table.add_row(["Total", "", format_pound(total)])
    table.align = "l"
    table.align["Cost"] = "r"

    print(table)


def print_compare(self, from_date: str | datetime = None, to_date: str | datetime = None, energy_type: str | None = None):

    """
    Print a comparison table of all the calculation methods.

    Examples:
        >>> account.print_compare('Agile Octopus')
        >>> account.print_compare('Flexible Octopus', '2023-01-15', '2024-04-16')

    Args:
        from_date: The date from which to print the comparion.
        to_date: The date to which to print the comparion.
        energy_type: The energy type to use in the comparion.

    Returns:
        None
        
    """

    if energy_type is None:
        energy_type = self.energy_type
    self._check_energy_type_input(energy_type)

    data = self.calcs[energy_type]

    if from_date:
        from_date = _parse_date(from_date)
    else:
        from_date = self.MOVED_IN_AT

    if to_date:
        to_date = _parse_date(to_date, 1)
    else:
        to_date = _parse_date(add=-1)

    from_date = pd.to_datetime(from_date).to_datetime64()
    to_date = pd.to_datetime(to_date).to_datetime64()

    from_date_converted = (
        pd.to_datetime(from_date)
        .tz_localize("Europe/London")
        .tz_convert("UTC")
        .to_datetime64()
    )
    to_date_converted = (
        pd.to_datetime(to_date)
        .tz_localize("Europe/London")
        .tz_convert("UTC")
        .to_datetime64()
    )

    names = []
    totals = []
    for method in data["methods"]:
        try:
            datax = method["dataframe"]
            name = method["name"]
            cost_unit_rate = name + "_cost_unit_rate"
            cost_standing_charge = name + "_cost_standing_charge"


        except KeyError:
            raise ValueError(
                "The calculations for compare were not found. It probably means you need to use the run() method on the class before calling compare."
            )

        name = method["name"]
        names.append(name)

        extracted_aware = datax.loc[
            (datax["from"] < to_date_converted) & (datax["to"] > from_date_converted)
        ]

        extracted_naive = datax.loc[(datax["from"] < to_date) & (datax["to"] > from_date)]


        cost_rate = extracted_aware[cost_unit_rate].sum() / 100
        # standing charges and the number of days are worked out on the dataframe extracted using tz-naive dates
        cost_standing = extracted_naive[cost_standing_charge].sum() / 100
        subtotal = cost_rate + cost_standing
        vat = ((subtotal / 100) * 5).round(2)
        total = (subtotal + vat).round(2)

        totals.append(total)

    totals_in = np.array(totals)
    names_np = np.array(names)

    out = pd.DataFrame(
        ((totals_in[:, None] - totals_in) / np.abs(total) * 100).round(1),
        index=names_np,
        columns=names_np,
    )
    np.fill_diagonal(out.values, np.nan)

    totals_df = pd.DataFrame(totals_in, index=names, columns=["Total (£)"])

    totals_df_rows = pd.DataFrame(datax.shape[0], index=names, columns=["Rows"])

    table = pd.merge(out, totals_df, left_index=True, right_index=True)
    table = pd.merge(table, totals_df_rows, left_index=True, right_index=True)

    print(table.sort_values("Total (£)"))

    pass


def print_checks(self, energy_type: str | None = None):

    if energy_type is None:
        energy_type = self.energy_type
    self._check_energy_type_input(energy_type)

    data = self.calcs[energy_type]

    for method in data["methods"]:
        print("Total final rows for", method["name"], method["dataframe"].shape[0])
        _find_duplicates(method["dataframe"])


def _find_duplicates(table):
    try:
        print(pd.concat(g for _, g in table.groupby("from") if len(g) > 1))
    except ValueError:
        print("No duplicates found.")

    pass
