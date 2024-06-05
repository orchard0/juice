from juice import Juice

account = Juice('api_key', 'account_number')
account.update()
account.set_energy('electricity')
account.add_bill()
account.add_method_by_tariff_family(
    ['Flexible Octopus', 'Agile Octopus', 'Octopus Tracker'])
account.calculate()
account.print_bill('2024-05-01', '2024-06-01')
account.print_compare()
