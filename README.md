##### In the name of God, the entirely compassionate, the especially compassionate

# Juice

Calculate Octopus energy bills and compare different tariffs.

Prerequistes:

-   PostgreSQL [instructions are available here.](https://www.postgresql.org/download/ 'instructions are available here.')
-   Create a new PostrgeSQL database using `sudo -u $USER psql -c 'create database utilities;'`. By default the database will be called `utilities`.
-   Install Python dependencies using `sudo pip install pandas pyjanitor requests prettytable psycopg`.
-   Clone this repo using `git clone https://github.com/orchard0/juice.git` or simply download the folder.
-   Get the Octopus account number, it can be found, when logged in, under your name on the homepage. It is something like `A-1A2B3C4D`.
-   Get the Octopus Energy API key [here](https://octopus.energy/dashboard/new/accounts/personal-details/api-access 'here'). It is something like `sk_live_rvcndtwguaeohpfqbsk12345`.

You can use `example.py` to get started. Add the API key and account number.

```
from juice import Juice

account = Juice('api_key', 'account_number')
account.update()
account.set_energy('electricity')
account.add_bill()
account.add_method_by_tariff_family(
    ['Flexible Octopus', 'Octopus Tracker', 'Intelligent Octopus Go'])
account.calculate()
account.print_bill('2024-04-17', '2024-05-17')
account.print_compare('2024-04-17', '2024-05-17')

```

Let's break it down to see what's happening.

`from juice import Juice`

1. import the juice library

`account = Juice('api_key', 'account_number')`

2. Create a `Juice` constructor using the API key and account number and assign it to a variable in this case called `account`.

`account.update()`

3. By calling the `update()` method the account details are fetched from the Octopus Energy API and saved as a `.json` file to the `Accounts` folder in the same directory as the script. Also:

    1. Fetch all the consumption data for all the energy types (gas and electricity) as applicable.
    2. Fetch all tariff data that applies to the account.
    3. Update the list of all the current products available from Octopus Energy.

This may take a few minutes the first time you run it as there could be a lot of data to fetch from the Octopus Energy API.

`account.set_energy('electricity')`

1. This will set the subsequent methods to use the energy type 'electricity'. You can also add `enengy_type=electricity` to the methods directly if you need to.

`account.add_bill()`

5. Add your bill to the calculations list.

`account.add_method_by_tariff_family(['Flexible Octopus', 'Octopus Tracker', 'Intelligent Octopus Go'])`

6. Add `Flexible Octopus`, `Octopus Tracker` and `Intelligent Octopus Go` to the calculations list. This is useful for comparing tariffs or finding out what the bill would have been on these tariffs.

`account.calculate()`

7. `calculate()` must be called prior to any `print_` method. Calling the `calculate()` method computes all the information necessary for comparing and printing tariffs.

`account.print_bill('2024-04-17', '2024-05-17')`

8. Print the bill for the period between 17th April 2024 and 17th May 2024.

```
+-------------------+-------------+--------+
| Bill              | Consumption |   Cost |
+-------------------+-------------+--------+
| Total consumption | 95.3kWh     | £14.56 |
| Standing charge   | 30 days     | £10.44 |
| Subtotal          |             | £25.00 |
| Vat at 5%         |             |  £1.25 |
+-------------------+-------------+--------+
| Total             |             | £26.25 |
+-------------------+-------------+--------+
```

This will be the same as the real bill for the same period:

<img src="./assets/bill.png" alt="A real bill screenshot showing that the print_bill() method will show the the same figures as the real bill." width="350"/>

<br />

`account.print_compare('2024-04-17', '2024-05-17')`

9. Print the comparison for all the tariffs added earlier including the bill between 17th April 2024 and 17th May 2024.

```
                        Bill  Flexible Octopus  Octopus Tracker  Intelligent Octopus Go  Total (£)
Bill                     NaN             -30.0            -14.2                   -20.3      26.24
Flexible Octopus        30.0               NaN             15.8                     9.7      36.10
Octopus Tracker         14.2             -15.8              NaN                    -6.1      30.91
Intelligent Octopus Go  20.3              -9.7              6.1                     NaN      32.90
```

I'm on the Agile Octopus tariff so this comparison shows that my bill was 30% cheaper than it would have been on the Flexible Octopus tariff and 14.2% cheaper than Octopus Tracker tariff. The table also shows what the cost would have been on the different tariffs. For example had I been on the latest Octopus Tracker tariff during this period I would have paid £30.91.

# API

Full API reference coming soon.
