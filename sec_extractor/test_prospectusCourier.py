import unittest
from unittest.mock import MagicMock
from unittest import mock
import functools
import sec_extractor
import datetime as dt
import os
import glob
import shutil
from pathlib import Path
import pandas as pd
import numpy as np


class testProspectusCourier(unittest.TestCase):


    def test_get_list_url_files(self):

        configuration_manager = sec_extractor.configurationManager()
        config = configuration_manager.get_config()

        db_manager = sec_extractor.databaseManager(config)

        prospectus_courier = sec_extractor.prospectusCourier(config, db_manager)
        prospectus_courier.config['prospectus']['start_year'] = 2020
        prospectus_courier.get_list_quarters()
        prospectus_courier.get_list_url_files()

        self.assertEqual(prospectus_courier.url_file_list, ['https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets/2020q1_rr1.zip', 'https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets/2020q2_rr1.zip', 'https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets/2020q3_rr1.zip', 'https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets/2020q4_rr1.zip', 'https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets/2021q1_rr1.zip', 'https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets/2021q2_rr1.zip', 'https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets/2021q3_rr1.zip'])


    def test_get_quarter_prospectuses(self):

        try:
            os.remove("test_assets\\test_prospectuses\\2021-03-31\\sub.tsv")
        except OSError:
            pass

        try:
            os.remove("test_assets\\test_prospectuses\\2021-06-30\\sub.tsv")
        except OSError:
            pass

        try:
            os.remove("test_assets\\test_prospectuses\\2021-03-31\\num.tsv")
        except OSError:
            pass

        try:
            os.remove("test_assets\\test_prospectuses\\2021-06-30\\num.tsv")
        except OSError:
            pass

        configuration_manager = sec_extractor.configurationManager()
        config = configuration_manager.get_config()

        db_manager = sec_extractor.databaseManager(config)

        #Mocks
        #today = dt.datetime.today()
        config['network_drives']['zip_prospectuses'] = 'test_assets\\test_zip_prospectuses'
        #db_manager.get_most_recent_prospectus_date = MagicMock(return_value=dt.datetime(today.year,1,1))
        config['network_drives']['prospectuses'] = 'test_assets\\test_prospectuses'
        #config['prospectus']['start_year'] = today.year

        prospectus_courier = sec_extractor.prospectusCourier(config, db_manager)
        #prospectus_courier.get_list_quarters()
        #prospectus_courier.get_list_url_files()
        #prospectus_courier.download_zip_files()
        #prospectus_courier.filter_zip_files()
        prospectus_courier.filtered_zip_files = ['test_assets\\test_zip_prospectuses\\2021-03-31.zip', 'test_assets\\test_zip_prospectuses\\2021-06-30.zip']
        prospectus_courier.get_quarter_prospectuses()

        self.assertEqual(True, Path("test_assets\\test_prospectuses\\2021-03-31\\sub.tsv").is_file())
        self.assertEqual(True, Path("test_assets\\test_prospectuses\\2021-06-30\\sub.tsv").is_file())
        self.assertEqual(True, Path("test_assets\\test_prospectuses\\2021-03-31\\num.tsv").is_file())
        self.assertEqual(True, Path("test_assets\\test_prospectuses\\2021-06-30\\num.tsv").is_file())


    def test_get_prospectuses_data(self):


        configuration_manager = sec_extractor.configurationManager()
        config = configuration_manager.get_config()

        db_manager = sec_extractor.databaseManager(config)

        prospectus_courier = sec_extractor.prospectusCourier(config, db_manager)
        prospectus_courier.prospectus_paths = ['test_assets\\test_get_prospectuses_data']
        prospectus_courier.config['series_to_index'] = {"a": None, "b": None}
        pivot_df = prospectus_courier.get_prospectuses_data(prospectus_courier.prospectus_paths[0])

        compare_df = pd.DataFrame({'adsh': ['a', 'b', 'c','d','e','f','g','h'],
        'class': ['a', 'b', 'c','d','e','f','g','h'],
        'series': ['a', 'b', 'c','d','e','f','g','h'],
        'cik': ['a', 'b', 'c','d','e','f','g','h'],
        'name': ['T. ROWE PRICE TAX-EXEMPT MONEY FUND, INC.','T. ROWE PRICE TAX-EXEMPT MONEY FUND, INC.','JPMORGAN TRUST I','JPMORGAN TRUST I','JPMORGAN TRUST I','JPMORGAN TRUST I','JPMORGAN TRUST I','JPMORGAN TRUST I'],
        'effdate': [dt.datetime(2021,3,16),dt.datetime(2021,3,16),dt.datetime(2021,3,1),dt.datetime(2021,3,1),dt.datetime(2021,3,1),dt.datetime(2021,3,1),dt.datetime(2021,3,1),dt.datetime(2021,3,1)],
        'filed': [dt.datetime(2021,3,16),dt.datetime(2021,3,16),dt.datetime(2021,2,26),dt.datetime(2021,2,26),dt.datetime(2021,2,26),dt.datetime(2021,2,26),dt.datetime(2021,2,26),dt.datetime(2021,2,26)],
        'form': ['497', '497', '485BPOS','485BPOS','485BPOS','485BPOS','485BPOS','485BPOS'],
        'AverageAnnualReturnSinceInception': ['a', 'b', 'c','d','e','f','g','h'],
        'AverageAnnualReturnYear01': ['a', 'b', 'c','d','e','f','g','h'],
        'AverageAnnualReturnYear05': ['a', 'b', 'c','d','e','f','g','h'],
        'ExpensesOverAssets': ['a', 'b', 'c','d','e','f','g','h']})

        pd.testing.assert_frame_equal(pivot_df, compare_df)


    def test_get_prospectus_table_data(self):

        configuration_manager = sec_extractor.configurationManager()
        config = configuration_manager.get_config()
        config['network_drives']['database'] = "test_assets\\test_get_prospectus_table_data\\database"

        db_manager = sec_extractor.databaseManager(config)
        db_manager.create_tables()

        prospectus_courier = sec_extractor.prospectusCourier(config, db_manager)
        prospectus_courier.config['series_to_index'] = {"a": " S&P SmallCap 600 Index", "b": "S&P MidCap 400 Index"}
        prospectus_courier.prospectus_paths = ['test_assets\\test_get_prospectus_table_data\\paths\\2012-03-31', 'test_assets\\test_get_prospectus_table_data\\paths\\2020-12-31']
        prospectus_courier.obtain_insert_prospectus_data()


if __name__ == "__main__":

    unittest.main()