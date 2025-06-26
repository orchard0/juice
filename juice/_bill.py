from datetime import datetime
from prettytable import PrettyTable
import math
from pytz import timezone
from psycopg import DatabaseError
from ._psql import query_calorific_values, retrive_unit_rates, retrive_consumption
from ._utils import _parse_date, _format_date
import pandas as pd
import numpy as np
import janitor


def _get_consumption(psql_config, dbname, from_date, to_date):

    s = retrive_consumption(psql_config, dbname, from_date, to_date)
    if not s:
        return pd.DataFrame()
    r = pd.DataFrame(s, columns=["consumption", "from", "to"])
    r["consumption"] = r["consumption"].astype(np.float64)
    r["from"] = r["from"].dt.tz_convert(None)
    r["to"] = r["to"].dt.tz_convert(None)
    return r.sort_values("from")


def _join(psql_config, dbname, dataframe, from_date, to_date, LDZ=None):

    def get_unit_rates(dbname, from_date, to_date, payment_method="DIRECT_DEBIT"):

        if dbname == "calorific_values":
            s = query_calorific_values(psql_config, dbname, LDZ, from_date, to_date)
        else:
            s = retrive_unit_rates(
                psql_config, dbname, payment_method, from_date, to_date
            )

        if not s or len(s) == 0:
            raise ValueError

        u = pd.DataFrame(s, columns=["rate", "valid_from", "valid_to"])

        u["rate"] = u["rate"].astype(np.float64)

        with pd.option_context("future.no_silent_downcasting", True):
            u = u.fillna(pd.Timestamp.utcnow()).infer_objects(copy=False)

        u["valid_from"] = u["valid_from"].dt.tz_convert(None)
        u["valid_to"] = u["valid_to"].dt.tz_convert(None)

        return u

    rates = get_unit_rates(
        dbname,
        from_date,
        to_date,
    )

    from_date = pd.to_datetime(from_date).to_datetime64()
    to_date = pd.to_datetime(to_date).to_datetime64()

    joined = rates.conditional_join(
        dataframe,
        ("valid_to", "from", ">"),
        ("valid_from", "to", "<"),
        df_columns="rate",
    )

    filtered_by_dates = joined.loc[
        (joined["from"] >= from_date) & (joined["from"] < to_date)
    ]

    return filtered_by_dates


def _join_custom(rate, dataframe, from_date, to_date):

    def get_unit_rates(rate, from_date, to_date):

        s = [
            (
                rate,
                from_date,
                to_date,
            )
        ]

        if not s or len(s) == 0:
            raise ValueError

        u = pd.DataFrame(s, columns=["rate", "valid_from", "valid_to"])

        u["rate"] = u["rate"].astype(np.float64)

        with pd.option_context("future.no_silent_downcasting", True):
            u = u.fillna(pd.Timestamp.utcnow()).infer_objects(copy=False)

        u["valid_from"] = u["valid_from"].dt.tz_convert(None)
        u["valid_to"] = u["valid_to"].dt.tz_convert(None)

        return u

    rates = get_unit_rates(
        rate,
        from_date,
        to_date,
    )

    from_date = pd.to_datetime(from_date).to_datetime64()
    to_date = pd.to_datetime(to_date).to_datetime64()

    joined = rates.conditional_join(
        dataframe,
        ("valid_to", "from", ">"),
        ("valid_from", "to", "<"),
        df_columns="rate",
    )

    filtered_by_dates = joined.loc[
        (joined["from"] >= from_date) & (joined["from"] < to_date)
    ]

    return filtered_by_dates


def _merge_dataframes(methods):

    result = methods.copy()

    methods_del = []
    for index, method in enumerate(result["methods"]):

        data = method["cost_types"]

        try:
            ur = data["_standard_unit_rates"]
            sc = data["_standing_charges"]
            df = pd.merge(ur, sc[["rate", "from"]], on="from")
            new_column_names = {
                "rate_x": method["name"] + "_unit_rate",
                "rate_y": method["name"] + "_standing_charge",
            }

            df.rename(columns=new_column_names, inplace=True)
        except KeyError as e:
            if e.args[0] == "_standard_unit_rates":
                df = data["_standing_charges"]
                new_column_names = {"rate": method["name"] + "_standing_charge"}
                df.rename(columns=new_column_names, inplace=True)
            elif e.args[0] == "_standing_charges":
                df = ur
                new_column_names = {"rate": method["name"] + "_unit_rate"}
                df.rename(columns=new_column_names, inplace=True)

        except (ValueError, TypeError):
            methods_del.append(method)
            continue

        try:
            cv = result["_calorific_values"]

            df = pd.merge(
                df,
                cv[["rate", "from"]],
                on="from",
            )
            df.rename(columns={"rate": "calorific_values"}, inplace=True)
        except KeyError:
            pass

        method["dataframe"] = df

    for index in methods_del:
        result["methods"].remove(index)

    return result


def _calc_costs(dataframes, energy_type):
    data_in = _merge_dataframes(dataframes)
    result = data_in.copy()

    for method in result["methods"]:

        data = method["dataframe"]

        rate_unit = method["name"] + "_unit_rate"
        rate_standing_charge = method["name"] + "_standing_charge"
        cost_unit_rate = method["name"] + "_cost_unit_rate"
        cost_standing_charge = method["name"] + "_cost_standing_charge"
        total = method["name"] + "_total"

        a = pd.DataFrame(data[["from", "to", "consumption"]])

        if energy_type == "gas":
            a["calorific_value"] = data["calorific_values"]
            a["consumption_units"] = a["consumption"].round(2)
            a["consumption_rounded"] = (
                a["consumption"] * np.float64(1.02264) * a["calorific_value"]
            ) / np.float64(3.6)

        else:
            a["consumption_rounded"] = a["consumption"].round(2)

        #
        try:
            a[cost_unit_rate] = data[rate_unit] * a["consumption_rounded"]
        except KeyError:
            pass

        try:
            a[cost_standing_charge] = data[rate_standing_charge] * (
                (a["to"] - a["from"]).dt.seconds / 86400
            )

        except KeyError:
            pass

        try:
            a[total] = a[cost_unit_rate] + a[cost_standing_charge]
        except KeyError:
            try:
                a[total] = a[cost_unit_rate]
            except KeyError:
                a[total] = a[cost_standing_charge]

        method["dataframe"] = a

    return result


@staticmethod
def _run_config(psql_config, data, energy_type, from_date, to_date, LDZ=None):

    def min_max_dates_and_size_check(data, name, consumption_size):
        min_date = utc.localize(data["from"].min().to_pydatetime())
        max_date = utc.localize(data["to"].max().to_pydatetime())

        if not (
            min_date <= from_date
            and max_date >= to_date
            and consumption_size == data.shape[0]
        ):
            missing_days = abs((min_date - from_date).days + (max_date - to_date).days)
            raise DatabaseError(
                f"There is {missing_days} day(s) of missing data for {name}. The data was available from {_format_date(min_date)} to {_format_date(max_date)}. The required range is {_format_date(from_date)} to {_format_date(to_date)}. Is the database up to date? Try running update()."
            )

    utc = timezone("UTC")

    print(
        "Getting consumption figures from",
        _format_date(from_date),
        "to",
        _format_date(to_date),
    )

    consumption_df = pd.concat(
        _get_consumption(psql_config, dbname, from_date, to_date)
        for dbname in data["consumption_dbs"]
    ).sort_values("from")

    consumption_size = consumption_df.shape[0]
    min_max_dates_and_size_check(consumption_df, "consumption", consumption_size)

    print("Total rows for consumption:", consumption_df.shape[0])

    if energy_type == "gas":
        if not LDZ:
            raise ValueError(
                "LDZ was not found for the property. Please add it manually in the Juice constructor."
            )
        data["_calorific_values"] = _join(
            psql_config, "calorific_values", consumption_df, from_date, to_date, LDZ
        ).sort_values("from")
        min_max_dates_and_size_check(
            data["_calorific_values"], "calorific values", consumption_size
        )

    for method in data["methods"]:
        agreements = sorted(method["agreements"], key=lambda d: d["valid_from"])

        print("Calculating", method["name"])
        for cost_type in method["cost_types"]:

            tables = []

            for agreement in agreements:
                if agreement["valid_from"] == agreement["valid_to"]:
                    continue

                valid_from = agreement["valid_from"]
                valid_to = agreement["valid_to"]

                if not valid_to:
                    valid_to = datetime.now()

                try:
                    standing_charge = agreement["standing_charge"]
                    unit_rate = agreement["unit_rate"]

                    if cost_type == "_standard_unit_rates":
                        rate = unit_rate
                    else:
                        rate = standing_charge

                    joined = _join_custom(rate, consumption_df, valid_from, valid_to)
                    tables.append(joined)

                # if there is no standing charge and unit rate specified then look up the information in the database
                except KeyError:
                    tariff_code = agreement["tariff_code"] + cost_type

                    try:
                        joined = _join(
                            psql_config,
                            tariff_code,
                            consumption_df,
                            valid_from,
                            valid_to,
                        )

                        tables.append(joined)
                    except ValueError:
                        continue

            try:
                x = pd.concat(tables).sort_values("from")
                method["cost_types"][cost_type] = x
                # print(method["cost_types"][cost_type])
                min_max_dates_and_size_check(
                    method["cost_types"][cost_type], method["name"], consumption_size
                )
            except ValueError:
                pass

    return {
        **data,
        "dataframe": _calc_costs(data, energy_type),
        "from_date": from_date,
        "to_date": to_date,
    }


def calculate(
    self,
    from_date: None | str | datetime = None,
    to_date: None | str | datetime = None,
    energy_type: None | str = None,
):
    """
    Calculate costs for the methods added.

    Args:
        from_date: A date from which to begin calculations.
        to_date: A date to which calculate.

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
        to_date = _parse_date(to_date)
    else:
        to_date = _parse_date(add=-2)

    if from_date > to_date:
        raise ValueError(
            f"from_date {_format_date(from_date)} is larger than the to_date {_format_date(to_date)} "
        )

    data["from_date"] = from_date
    data["to_date"] = to_date

    _check_method_dates(data["methods"], from_date, to_date)

    data = _run_config(
        self.psql_config, data, energy_type, from_date, to_date, self.LDZ
    )


def _check_method_dates(data, from_date, to_date):

    invalid_methods = []
    utc = timezone("utc")
    earliest_date = utc.localize(datetime(1900, 1, 1))

    for method in data:
        method_from_date = method["from_date"]
        method_to_date = method["to_date"]
        method_to_date_display = method_to_date
        if not method_to_date:
            method_to_date = utc.localize(datetime(3000, 1, 1))
            method_to_date_display = math.inf
        if not (
            from_date <= method_to_date
            and to_date >= method_from_date
            and from_date >= method_from_date
            and to_date <= method_to_date
        ):
            if method_from_date > earliest_date:
                earliest_date = method_from_date
            invalid_methods.append(
                {
                    "name": method["name"],
                    "from": _format_date(method_from_date),
                    "to": _format_date(method_to_date_display),
                }
            )

    invalid_methods = sorted(invalid_methods, key=lambda d: d["from"])
    if invalid_methods:
        table = PrettyTable(["Method", "from", "to"])

        main_msg = f"The calculation's date range {_format_date(from_date)} to {_format_date(to_date)} does not fall within the following method(s) date range(s):\n"
        earliest_date_msg = f"\nThe earliest calculation date is {_format_date(earliest_date)}. Either remove the offending method(s) or change the calculation dates."
        for invalid in invalid_methods:
            table.add_row([invalid["name"], invalid["from"], invalid["to"]])

        raise ValueError(main_msg + str(table) + earliest_date_msg)
