import pandas as pd


def get_users_accounts(sacct_output_data):
    return sacct_output_data.loc[:, ['User', 'Account']].sort_values(
        ['User', 'Account']).drop_duplicates().set_index('User')
