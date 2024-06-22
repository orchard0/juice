from juice import utils
import getpass

config = {
    'user': getpass.getuser(),  # this will get your username
    'password': 'password_string',
    'host': '127.0.0.1',
    'port': '5432',
    'dbname': 'utilities',
    'autocommit': True,
}

utils.setup_ldz_table(config)
