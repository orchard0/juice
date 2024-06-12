from datetime import datetime, timezone
import math
from ._psql import query_calorific_values, retrive_unit_rates, retrive_consumption
from ._utils import parse_date, format_date
import pandas as pd
import numpy as np
import janitor


def find_duplicates(table):
    try:
        print(pd.concat(g for _, g in table.groupby("from") if len(g) > 1))
    except ValueError:
        print("No duplicates found.")


def get_consumption(psql_config, dbname, from_date, to_date):

    s = retrive_consumption(psql_config, dbname, from_date, to_date)
    if not s:
        return pd.DataFrame()
    r = pd.DataFrame(s, columns=["consumption", "from", "to"])
    r["consumption"] = r["consumption"].astype(np.float64)
    r["from"] = r["from"].dt.tz_convert(None)
    r["to"] = r["to"].dt.tz_convert(None)
    return r.sort_values("from")


def join(psql_config, dbname, dataframe, from_date, to_date, LDZ=None):

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


def combined(info):

    result = info.copy()

    methods_del = []
    for index, method in enumerate(result["methods"]):

        data = method["cost_types"]

        try:
            ur = pd.concat(data["_standard_unit_rates"]).sort_values("from")
            sc = pd.concat(data["_standing_charges"]).sort_values("from")
            df = pd.merge(ur, sc[["rate", "from"]], on="from")
            new_column_names = {
                "rate_x": method["name"] + "_unit_rate",
                "rate_y": method["name"] + "_standing_charge",
            }

            df.rename(columns=new_column_names, inplace=True)
        except KeyError as e:
            if e.args[0] == "_standard_unit_rates":
                df = pd.concat(data["_standing_charges"]).sort_values("from")
                new_column_names = {"rate": method["name"] + "_standing_charge"}
                df.rename(columns=new_column_names, inplace=True)
            elif e.args[0] == "_standing_charges":
                df = ur
                new_column_names = {"rate": method["name"] + "_unit_rate"}
                df.rename(columns=new_column_names, inplace=True)

        except ValueError:
            methods_del.append(method)
            continue

        try:
            cv = pd.concat(result["_calorific_values"]).sort_values("from")

            df = pd.merge(
                df,
                cv[["rate", "from"]],
                on="from",
            )
            df.rename(columns={"rate": "calorific_values"}, inplace=True)
        except ValueError:
            pass

        method["dataframe"] = df

    for index in methods_del:
        result["methods"].remove(index)

    return result


def calc_costs(dataframes):
    data_in = combined(dataframes)
    result = data_in.copy()

    for method in result["methods"]:

        data = method["dataframe"]

        rate_unit = method["name"] + "_unit_rate"
        rate_standing_charge = method["name"] + "_standing_charge"
        cost_unit_rate = method["name"] + "_cost_unit_rate"
        cost_standing_charge = method["name"] + "_cost_standing_charge"
        total = method["name"] + "_total"

        a = pd.DataFrame(data[["from", "to", "consumption"]])

        if result["energy_type"] == "gas":
            a["calorific_value"] = data["calorific_values"]
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
def run_config(psql_config, data, from_date, to_date, LDZ=None):

    print(
        "Getting consumption figures from",
        format_date(from_date),
        "to",
        format_date(to_date),
    )

    consumption_df = pd.concat(
        get_consumption(psql_config, dbname, from_date, to_date)
        for dbname in data["consumption_dbs"]
    )

    print("Total rows for consumption:", consumption_df.shape[0])
    energy_type = data["energy_type"]

    if energy_type == "gas":
        if not LDZ:
            raise ValueError(
                "LDZ was not found for the property. Please add it manually in the Juice constructor."
            )
        joined = join(
            psql_config, "calorific_values", consumption_df, from_date, to_date, LDZ
        )
        data["_calorific_values"].append(joined)

    for method in data["methods"]:
        agreements = sorted(method["agreements"], key=lambda d: d["valid_from"])

        print("Calculating", method["name"])
        for agreement in agreements:
            if agreement["valid_from"] == agreement["valid_to"]:
                continue

            for cost_type in method["cost_types"]:

                tariff_code = agreement["tariff_code"] + cost_type

                valid_from = agreement["valid_from"]
                valid_to = agreement["valid_to"]

                if not valid_to:
                    valid_to = datetime.now()

                try:
                    joined = join(
                        psql_config, tariff_code, consumption_df, valid_from, valid_to
                    )

                    method["cost_types"][cost_type].append(joined)
                except ValueError:
                    continue

    return {
        **data,
        "dataframe": calc_costs(data),
        "from_date": from_date,
        "to_date": to_date,
    }


def calculate(self, from_date=None, to_date=None, energy_type=None):

    if energy_type is None:
        energy_type = self.energy_type
    self.check_energy_type_input(energy_type)

    data = self.calcs[energy_type]

    if from_date:
        from_date = parse_date(from_date)
    else:
        from_date = self.MOVED_IN_AT

    if to_date:
        to_date = parse_date(to_date, 1)
    else:
        to_date = parse_date(add=1)

    if from_date > to_date:
        raise ValueError(
            f"from_date {format_date(from_date)} is larger than the to_date {format_date(to_date)} "
        )

    data["from_date"] = from_date
    data["to_date"] = to_date

    def check_method_dates(data, from_date, to_date):

        for method in data:
            method_from_date = method["from_date"]
            method_to_date = method["to_date"]
            method_to_date_display = method_to_date
            if not method_to_date:
                method_to_date = datetime(3000, 1, 1, tzinfo=timezone.utc)
                method_to_date_display = math.inf
            if not (
                from_date <= method_to_date
                and to_date >= method_from_date
                and from_date >= method_from_date
                and to_date <= method_to_date
            ):
                raise ValueError(
                    f"The calculation's date range {format_date(from_date)} to {format_date(to_date)} do not fall within {method['name']}'s date range {format_date(method_from_date)} to {format_date(method_to_date_display)}"
                )

    check_method_dates(data["methods"], from_date, to_date)

    data = self.run_config(self.psql_config, data, from_date, to_date, self.LDZ)
