import unittest
from unittest.mock import MagicMock
from unittest import mock
import functools
import sec_extractor
import datetime as dt
from bs4 import BeautifulSoup


class testHoldingsCourier(unittest.TestCase):


    def test_filter_indexes(self):

        configuration_manager = sec_extractor.configurationManager()
        config = configuration_manager.get_config()

        db_manager = sec_extractor.databaseManager(config)
        db_manager.get_most_recent_qtr_end_date = MagicMock(return_value=dt.datetime(2021,6,30))

        index_courier = sec_extractor.indexCourier(config)

        index_courier.get_index_files = MagicMock(return_value=['test_assets\\test_filter_indexes\\2021-QTR1.tsv', 'test_assets\\test_filter_indexes\\2021-QTR2.tsv', 'test_assets\\test_filter_indexes\\2021-QTR3.tsv'])

        holdingsCourier = sec_extractor.holdingsCourier(config)
        holdingsCourier.filter_indexes(db_manager, index_courier)
        #holdingsCourier.index_files = MagicMock(return_value=['test_assets\\test_filter_indexes\\2021-QTR1.tsv', 'test_assets\\test_filter_indexes\\2021-QTR2.tsv', 'test_assets\\test_filter_indexes\\2021-QTR3.tsv'])
        #with mock.patch('sec_extractor.holdingsCourier.index_files', return_value=['test_assets\\test_filter_indexes\\2021-QTR1.tsv', 'test_assets\\test_filter_indexes\\2021-QTR2.tsv', 'test_assets\\test_filter_indexes\\2021-QTR3.tsv']):

        #    holdingsCourier.filter_indexes(db_manager, index_courier)

        self.assertEqual(holdingsCourier.filtered_index_files, ['test_assets\\test_filter_indexes\\2021-QTR2.tsv', 'test_assets\\test_filter_indexes\\2021-QTR3.tsv'])


    def test_get_report_urls(self):

        configuration_manager = sec_extractor.configurationManager()
        config = configuration_manager.get_config()

        holdingsCourier = sec_extractor.holdingsCourier(config)
        #Don't call holdingsCourier.filter_indexes, instead just impute filtered_index_files below
        holdingsCourier.filtered_index_files = ['test_assets\\test_get_report_urls\\2011-QTR1.tsv', 'test_assets\\test_get_report_urls\\2021-QTR2.tsv']
        holdingsCourier.config['ciks'] = ['a', 'b', 'c', 'd', 'e']
        holdingsCourier.get_report_urls()

        self.assertCountEqual(holdingsCourier.filtered_report_urls, [[{'filing_type': 'N-Q', 'url': 'a'}, {'filing_type': 'N-Q', 'url': 'b'}, {'filing_type': 'N-Q/A', 'url': 'c'}], [{'filing_type': 'NPORT-P', 'url': 'd'}, {'filing_type': 'NPORT-P', 'url': 'e'}, {'filing_type': 'NPORT-P/A', 'url': 'f'}]])


    def test_translate_period_end_quarter_end(self):

        configuration_manager = sec_extractor.configurationManager()
        config = configuration_manager.get_config()

        holdingsCourier = sec_extractor.holdingsCourier(config)

        self.assertEqual('2020-03-31', holdingsCourier.translate_period_end_quarter_end('2020-01-14'))
        self.assertEqual('2021-06-30', holdingsCourier.translate_period_end_quarter_end('2021-06-30'))
        self.assertEqual('2011-12-31', holdingsCourier.translate_period_end_quarter_end('2011-10-01'))
        self.assertEqual('2016-09-30', holdingsCourier.translate_period_end_quarter_end('2016-07-15'))


    def test_get_nq_series_data(self):
        #AMERICAN FIDELITY DUAL STRATEGY FUND INC. 2014

        configuration_manager = sec_extractor.configurationManager()
        config = configuration_manager.get_config()

        #Holdings courier
        holdings_courier = sec_extractor.holdingsCourier(config)
        holdings_courier.filtered_report_urls = [[{'filing_type': 'N-Q', 'url': 'a'}]]

        proxy_manager = sec_extractor.proxyManager()
        proxy_manager.set_http_session(config)
        response = proxy_manager.session.get(holdings_courier.filtered_report_urls[0][0]['url'], headers={'User-Agent': config['http_session']['user_agent']})

        #Transfer content to xml format
        xml = BeautifulSoup(response.content, 'lxml')
        dates_data, holdings_data = holdings_courier.get_nq_series_data('a', xml, 'N-Q')

        self.assertEqual(('2014-09-30', '2014-09-30'), dates_data)
        self.assertEqual(('a', 'N-Q', '2014-11-13', '2014-09-30', 'b', 'c'), holdings_data)

        ##############################################################################################################################
        #Vanguard S&P 500 Growth Index Fund 2011

        #Holdings courier
        holdings_courier = sec_extractor.holdingsCourier(config)
        holdings_courier.filtered_report_urls = [[{'filing_type': 'N-Q', 'url': 'a'}]]

        response = proxy_manager.session.get(holdings_courier.filtered_report_urls[0][0]['url'], headers={'User-Agent': config['http_session']['user_agent']})

        #Transfer content to xml format
        xml = BeautifulSoup(response.content, 'lxml')
        dates_data, holdings_data = holdings_courier.get_nq_series_data('a', xml, 'N-Q')

        self.assertEqual(('2010-11-30', '2010-12-31'), dates_data)
        self.assertEqual(('a', 'N-Q', '2011-01-31', '2010-11-30', 'b', 'c'), holdings_data)

        ##############################################################################################################################
        #Vanguard Intermediate-Term Bond Index Fund 2018

        #Holdings courier
        holdings_courier = sec_extractor.holdingsCourier(config)
        holdings_courier.filtered_report_urls = [[{'filing_type': 'N-Q', 'url': 'a'}]]

        response = proxy_manager.session.get(holdings_courier.filtered_report_urls[0][0]['url'], headers={'User-Agent': config['http_session']['user_agent']})

        #Transfer content to xml format
        xml = BeautifulSoup(response.content, 'lxml')
        dates_data, holdings_data = holdings_courier.get_nq_series_data('a', xml, 'N-Q')

        self.assertEqual(('2018-09-30', '2018-09-30'), dates_data)
        self.assertEqual(('a', 'N-Q', '2018-11-29', '2018-09-30', 'b', 'c'), holdings_data)

        ##############################################################################################################################
        #Blackrock Bond Index Fund 2012

        #Holdings courier
        holdings_courier = sec_extractor.holdingsCourier(config)
        holdings_courier.filtered_report_urls = [[{'filing_type': 'N-Q', 'url': 'a'}]]

        response = proxy_manager.session.get(holdings_courier.filtered_report_urls[0][0]['url'], headers={'User-Agent': config['http_session']['user_agent']})

        #Transfer content to xml format
        xml = BeautifulSoup(response.content, 'lxml')
        dates_data, holdings_data = holdings_courier.get_nq_series_data('a', xml, 'N-Q')

        self.assertEqual(('2012-03-31', '2012-03-31'), dates_data)
        self.assertEqual(('a', 'N-Q', '2012-05-25', '2012-03-31', 'b', 'c'), holdings_data)

        ##############################################################################################################################
        #Blackrock Bond Index Fund 2012

        #Holdings courier
        holdings_courier = sec_extractor.holdingsCourier(config)
        holdings_courier.filtered_report_urls = [[{'filing_type': 'N-Q', 'url': 'a'}]]

        response = proxy_manager.session.get(holdings_courier.filtered_report_urls[0][0]['url'], headers={'User-Agent': config['http_session']['user_agent']})

        #Transfer content to xml format
        xml = BeautifulSoup(response.content, 'lxml')
        dates_data, holdings_data = holdings_courier.get_nq_series_data('a', xml, 'N-Q')

        self.assertEqual(('2012-03-31', '2012-03-31'), dates_data)
        self.assertEqual(('a', 'N-Q', '2012-05-25', '2012-03-31', 'b', 'c'), holdings_data)

        ##############################################################################################################################
        #Schwab 1000 Index Fund 2011

        #Holdings courier
        holdings_courier = sec_extractor.holdingsCourier(config)
        holdings_courier.filtered_report_urls = [[{'filing_type': 'N-Q', 'url': 'a'}]]

        response = proxy_manager.session.get(holdings_courier.filtered_report_urls[0][0]['url'], headers={'User-Agent': config['http_session']['user_agent']})

        #Transfer content to xml format
        xml = BeautifulSoup(response.content, 'lxml')
        dates_data, holdings_data = holdings_courier.get_nq_series_data('a', xml, 'N-Q')

        self.assertEqual(('2011-01-31', '2011-03-31'), dates_data)
        self.assertEqual(('a', 'N-Q', '2011-03-30', '2011-01-31', 'b', 'c'), holdings_data)


    def test_get_nport_series_data(self):
        #Vanguard Extended Market Index Fund 2021

        configuration_manager = sec_extractor.configurationManager()
        config = configuration_manager.get_config()

        #Holdings courier
        holdings_courier = sec_extractor.holdingsCourier(config)
        holdings_courier.filtered_report_urls = [[{'filing_type': 'NPORT-P', 'url': 'a'}]]

        proxy_manager = sec_extractor.proxyManager()
        proxy_manager.set_http_session(config)
        response = proxy_manager.session.get(holdings_courier.filtered_report_urls[0][0]['url'], headers={'User-Agent': config['http_session']['user_agent']})

        #Transfer content to xml format
        xml = BeautifulSoup(response.content, 'lxml')
        dates_data, holdings_data = holdings_courier.get_nport_data('a', xml, 'NPORT-P')

        self.assertEqual(('2021-06-30', '2021-06-30'), dates_data)
        self.assertEqual(('a', 'NPORT-P', '2021-08-30', '2021-06-30', 'b', 'c'), holdings_data)

        #Invesco Oppenheimer Mid Cap Value Fund 2019

        #Holdings courier
        holdings_courier = sec_extractor.holdingsCourier(config)
        holdings_courier.filtered_report_urls = [[{'filing_type': 'NPORT-P', 'url': 'a'}]]

        response = proxy_manager.session.get(holdings_courier.filtered_report_urls[0][0]['url'], headers={'User-Agent': config['http_session']['user_agent']})

        #Transfer content to xml format
        xml = BeautifulSoup(response.content, 'lxml')
        dates_data, holdings_data = holdings_courier.get_nport_data('a', xml, 'NPORT-P')

        self.assertEqual(('2019-10-31', '2019-12-31'), dates_data)
        self.assertEqual(('a', 'NPORT-P', '2019-12-30', '2019-10-31', 'b', 'c'), holdings_data)



if __name__ == "__main__":

    unittest.main()