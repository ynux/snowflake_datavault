import os
from generate_code import helpers

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_dir = os.path.join(parent_dir, 'conf')
sample_configfile = os.path.join(config_dir, "config.ini.sample")


def test_config_read():
    sample_config = helpers.read_config('db_dialect', config_ini="config.ini.sample")
    assert sample_config['dialect'] == 'sqlite'
    sample_config = helpers.read_config('metadata', config_ini="config.ini.sample")
    assert sample_config == {
        "user": 'XXX',
        "password": 'XXX',
        "account": 'XXX.xxxregion',
        "database": 'test_db',
        "schema": 'DV_MTD',
        "warehouse": 'xxx'
    }
