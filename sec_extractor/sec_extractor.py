import logging
import getpass
from bs4 import BeautifulSoup
import requests
import time
import json
import datetime as dt
import sqlite3
import urllib.parse
from urllib.request import urlretrieve
import warnings
import edgar
import functools
import os
import glob
from pathlib import Path
import pandas as pd
import re
import zipfile
import numpy as np
import sys


class configurationManager():
    '''
    Imports all configuration information from repository
    network_drives: drive locations that determine where output files should be placed
    http_session: the proxy server domain to be used, and the user-agent email for which the request is attributed (see https://www.sec.gov/os/accessing-edgar-data)
    holdings_tags: the tags in the holdings reports (N-Q (used pre-~2020) and N-PORT (used post-~2020)) that are used to get the holdings data
    '''

    def __init__(self):

        with open('config.json', 'r') as configuration_file:

            self.config = json.load(configuration_file)

        if int(self.config['index']['start_year']) < 1993:
            raise Exception('SEC Index start year cannot be less than 1993')

        if int(self.config['prospectus']['start_year']) < 2011:
            raise Exception('Prospectus start year cannot be less than 2011')


    def get_config(self):

        return self.config


class logManager():
    '''Configures log file, where all runtime notes will be published'''

    def __init__(self, config):

        self.log_file = config['network_drives']['log'] + r'\\' + 'sec_extractor.log'


    def config_log(self):

        logging.basicConfig(format='%(asctime)s || %(levelname)s: %(message)s', datefmt="%m-%d-%Y %I:%M %p", filename=self.log_file, level=logging.DEBUG)


    def declare_computer_user(self):

        computer = os.getenv("COMPUTERNAME").upper()
        logging.info(f'Computer running sec_extractor.py: {computer}')

        user = getpass.getuser().upper()
        logging.info(f'User running sec_extractor.py: {user}')


class proxyManager():
    '''Creates an HTTP session object that uses the proxy domain specified in the config_proxy_domain.json file'''

    def __init__(self):

        self.session = None


    def set_http_session(self, config):

        warnings.filterwarnings("ignore")

        proxy_exist = input("Do you need to accomodate a proxy server? Please input yes or no: ")

        if proxy_exist == "yes":

            #Input credentials
            uid = input("Input your uid: ")
            password = getpass.getpass("Input your LAN password: ")
            password = urllib.parse.quote(password)

            #Declare proxy address
            domain = config['http_session']['proxy_domain']
            proxyDict = {"http"  : 'http://{0}:{1}@{2}'.format(uid,password,domain),
                        "https" : 'https://{0}:{1}@{2}'.format(uid,password,domain)}

            #Create session object
            self.session = requests.Session()
            #Give our session the proxies (forwarding address)
            self.session.proxies = proxyDict
            #Ignore internal encryption
            self.session.verify = False
            self.session.trust_env = False
            logging.info('HTTP(S) session created')
            print('Credentials accepted; please check log file for further progress updates.')

            #Delete credentials
            del(uid)
            del(password)
            del(proxyDict)

        elif proxy_exist == "no":

            #Create session object
            self.session = requests.Session()
            #Ignore internal encryption
            self.session.verify = False
            self.session.trust_env = False
            logging.info('HTTP(S) session created')
            print('Credentials accepted; please check log file for further progress updates.')

        else:
            logging.info("yes or no is required as response to proxy inquiry.")
            sys.exit()


class indexCourier():
    '''
    Ensures all available SEC index files (beginning Q1 2011) are located in the network drive location specified in config.json
    The SEC index files state all SEC filings (10-K, N-PORT, N-Q, etc.) for a given quarter; in this application, they are used to find the N-PORT and N-Q (mutual fund holdings) filings for the desired funds
    '''

    def __init__(self, config):

        self.config = config
        self.index_files = None


    def obtain_index_files(self):

        start_year = int(self.config['index']['start_year'])
        if start_year < 2011:
            logging.info('Please note that the SEC only publishes prospectus data (expense ratios) starting 2011.')
        if start_year < 1993:
            logging.error('start_year in config.json needs to be greater than or equal to 1993, because that is the first year that the SEC published index files.')

        #Get all index files and place in configured network drive location
        index_start_time = time.time()
        edgar.download_index(self.config['network_drives']['index_files'], start_year, self.config['http_session']['user_agent'], skip_all_present_except_last=True)
        index_execution_time = (time.time() - index_start_time)/60
        logging.info(f'''SEC index file(s) download took {index_execution_time:.2f} minutes.''')


    def get_index_files(self):

        #Get all index files in network drive
        glob_str = self.config['network_drives']['index_files'] + '\\*.tsv'
        self.index_files = sorted(glob.glob(glob_str))

        return self.index_files


class holdingsCourier():
    '''
    Gets fund holdings data from N-Q (pre-2019) and NPORT-P (2019 onward) reports

    Structure of method use:

    - obtain_insert_holdings_data
        - get_nq_series_data (get_series_in_report, filter_to_desired_series)->needed because N-Q has multiple series per report
            - get_series_name_from_id
            - get_adsh
            - get_filed_date (format_date)
            - get_period_end_date (format_date)
            - translate_period_end_quarter_end
            - get_nq_net_assets
        - get_nport_data (get_series_in_report, filter_to_desired_series)->not really needed, just easier to reuse methods
            - get_adsh
            - get_filed_date (format_date)
            - get_period_end_date (format_date)
            - translate_period_end_quarter_end
            - get_nport_net_assets
    '''

    def __init__(self, config):


        self.translation_quarter_date = {"QTR1": [3,31], "QTR2": [6,30], "QTR3": [9,30], "QTR4": [12,31]}
        self.index_files = None
        self.filtered_index_files = []
        self.filtered_report_urls = []
        self.config = config


    ###### Methods that get or assist in getting report urls ######
    def translate_index_to_date(self, index_file):
        '''
        Gets quarter end date for index file
        @param index_file: absolute path of index file
        @return quarter end date of index file in datetime type
        '''

        index_file_name = Path(index_file).stem
        year_quarter = index_file_name.split('-')

        year = year_quarter[0]
        month = self.translation_quarter_date[year_quarter[1]][0]
        day = self.translation_quarter_date[year_quarter[1]][1]

        return dt.datetime(int(year), int(month), int(day))


    def filter_indexes(self, db_manager, index_courier):
        '''Create list of all index files that have yet to be inserted into database'''

        self.index_files = index_courier.get_index_files()

        try:
            most_recent_date = db_manager.get_most_recent_holdings_date()
        except:
            most_recent_date = dt.datetime(1993, 1, 1)

        for index_file in self.index_files:

            index_date = self.translate_index_to_date(index_file)

            if index_date >= most_recent_date:

                self.filtered_index_files.append(index_file)

        logging.info(f'(index files to be used for holdings insert: {self.filtered_index_files}')


    def get_report_urls(self):
        '''Get all report urls that match criteria and add to list of list of dict'''

        index_base_url = 'https://www.sec.gov/Archives/'

        #Parameters for reading in index files
        column_names=['cik', 'company', 'filing_type', 'filing_date', 'txt_endpoint', 'html_endpoint']
        use_columns = ['cik', 'filing_type', 'filing_date', 'txt_endpoint']
        dtype_dict = {'cik': str, 'filing_type': str, 'txt_endpoint': str}

        #Filing type filter
        filing_filter = self.config['filings']

        for index_file in self.filtered_index_files:

            chunks = []
            #Read data in chunks of 250,000, to ensure enough memory
            chunk_index = 0
            for chunk in pd.read_csv(index_file, sep='|', names=column_names, usecols=use_columns, dtype=dtype_dict, parse_dates=['filing_date'], infer_datetime_format=True, engine='python', chunksize=100000):
                chunk_index += 1
                logging.info(f'''Read in chunk {chunk_index} of 100,000 rows of data for {index_file}''')
                chunks.append(chunk)

            #Concatenate chunks of rows into single dataframe with all trades data
            index_df = pd.concat(chunks, ignore_index=True)

            #Get desired ciks from config json file
            ciks = self.config['ciks']
            #Strip leading zeros
            strip_ciks = [cik.lstrip("0") for cik in ciks]
            #Remove duplicate ciks
            strip_ciks = list(set(strip_ciks))
            #Keep only rows where cik is desired cik
            index_df = index_df[index_df['cik'].isin(strip_ciks)]

            #Keep only rows where filing type is desired filing type
            index_df = index_df[index_df['filing_type'].isin(filing_filter)]

            #Sort by date (allows for filing amendments to replace during database inserts)
            index_df.sort_values(by=['filing_date', 'filing_type'], ascending=True, inplace=True, ignore_index=True)

            #Convert txt_endpoint column to list
            #txt_endpoint_list = index_df['txt_endpoint'].to_list()

            #Add base url to each txt endpoint
            #url_list = [index_base_url + endpoint for endpoint in txt_endpoint_list]

            #Add base url to endpoint
            index_df['url'] = index_base_url + index_df['txt_endpoint']

            #Create list of dicts [{cik: "", filing_type: "", url: ""}, ...]
            date_type_index_df = index_df[['filing_type', 'url']]
            url_list = date_type_index_df.to_dict('records')

            #Append url list for this index to total url list
            self.filtered_report_urls.append(url_list)

        #flatten list of urls
        #self.filtered_report_urls = [url for index_file in self.filtered_report_urls for url in index_file]


    ###### Methods that get or assist in getting report data ######
    def get_series_in_report(self, xml):
        '''Gets all id's of series present in report'''

        series_list = []
        #Get all text associated with series-id tag
        series_ids = xml.find_all('series-id')

        for series in series_ids:

            #Append series id to list
            series_list.append(series.text[:10])

        return series_list


    def filter_to_desired_series(self, series_list):
        '''Filters list of series ids to those that are desired by user (noted in configuration json file)'''

        desired_series_list = self.config['series_to_index'].keys()
        filtered_series_list = []

        for series in series_list:

            if series in desired_series_list:

                filtered_series_list.append(series)

        return filtered_series_list


    def translate_period_end_quarter_end(self, period_end_date):
        '''Produces the quarter end date (yyyy-mm-dd) from any date (also needs to be formatted yyyy-mm-dd)'''

        month = period_end_date[5:7]

        if month in ['01', '02', '03']:
            return period_end_date[0:4] + '-' + '03' + '-' + '31'
        elif month in ['04', '05', '06']:
            return period_end_date[0:4] + '-' + '06' + '-' + '30'
        elif month in ['07', '08', '09']:
            return period_end_date[0:4] + '-' + '09' + '-' + '30'
        elif month in ['10', '11', '12']:
            return period_end_date[0:4] + '-' + '12' + '-' + '31'


    def get_adsh(self, xml):
        '''Get the accession number (adsh) which is unique to this report'''

        #Declare strings before and after adsh
        start = 'ACCESSION NUMBER:'
        end = 'CONFORMED'

        #Get text from tag that contains adsh
        summary_text = xml.find('acceptance-datetime').text

        #Takes text after start, then takes text before end from remaining, and finally strips any whitespace
        return (summary_text.split(start))[1].split(end)[0].strip()


    def format_date(self, date):
        '''Takes date in yyyymmdd format and places in yyyy-mm-dd'''

        return date[:4] + '-' + date[4:6] + '-' + date[6:8]


    def get_filed_date(self, xml):
        '''Get the date the report was filed'''

        #Declare strings before and after adsh
        start = 'FILED AS OF DATE:'
        end = 'DATE'

        #Get text from tag that contains adsh
        summary_text = xml.find('acceptance-datetime').text

        #Takes text after start, then takes text before end from remaining, and finally strips any whitespace
        unformatted_date = (summary_text.split(start))[1].split(end)[0].strip()
        formatted_date = self.format_date(unformatted_date)

        return formatted_date


    def get_period_end_date(self, xml):
        '''Get the date that is the final day of the 3-month (quarter) reporting period'''

        #Declare strings before and after adsh
        start = 'PERIOD OF REPORT:'
        end = 'FILED'

        #Get text from tag that contains adsh
        summary_text = xml.find('acceptance-datetime').text

        #Takes text after start, then takes text before end from remaining, and finally strips any whitespace
        unformatted_date = (summary_text.split(start))[1].split(end)[0].strip()
        formatted_date = self.format_date(unformatted_date)

        return formatted_date


    def get_series_name_from_id(self, series, xml):
        '''Gets series name from series id'''

        series_info = xml.find(text=re.compile(series)).find_next('series-name').text

        #Take series name (before new line and "C", which is always there (start of class id))
        return (series_info.split("\nC"))[0].strip()


    def get_nq_net_assets(self, series, xml, adsh):
        '''Attempts to get net assets for series from report'''

        series_name = self.get_series_name_from_id(series, xml)
        #Get words after first word (necessary because Fidelity will sometimes use @R symbol in second name, but not in first)
        if " " in series_name:
            split_series_name = series_name.split(" ", 1)[1]
        else:
            split_series_name = series_name

        #Get second occurrence of series name in document; necessary to find correct net assets tag
        #Making (what I believe to be true assumption) that first occurrence of series name is above html, and second will have net assets below it
        try:
            if bool(xml.find(text=re.compile('Name of Fund'))):
                series_section = xml.find(text=re.compile(split_series_name)).find_next(text=re.compile(split_series_name)).find_next(text=re.compile(split_series_name))
            else:
                series_section = xml.find(text=re.compile(split_series_name)).find_next(text=re.compile(split_series_name))
        except:
            logging.info(f'''{series_name} does not have a second occurrence in the document {adsh}, or at least that the bot could find.''')
            return None

        #Making assumption that there are no more than 10 instances of 'net assets' between the series section and the desired 'net assets'
        for i in range(10):

            try:

                #Try to find next occurrence of (case insensitive 'net assets')
                if i == 0:
                    next_net_assets = series_section.find_next(text=re.compile('(?i)net assets'))

                else:
                    next_net_assets = next_net_assets.find_next(text=re.compile('(?i)net assets'))

                #Take out headers of what we don't want
                if bool(re.search('(?i)percentage of net assets', next_net_assets)):

                    continue

                if bool(re.search('(?i)percentages shown are based on net assets', next_net_assets)):

                    continue

                #Make assumption that there are less than 10 columns between 'net assets' and the value
                for i in range(10):

                    try:

                        if i == 0:
                            next_td = next_net_assets.find_next('td')
                            next_td_text = next_td.text

                        else:
                            next_td = next_td.find_next('td')
                            next_td_text = next_td.text

                        #Match if contains ,### (this does exclude possibility for fund to have less than $1mil in assets (if in 000's))
                        if bool(re.search('(,\d{3})', next_td_text)):

                            #Extract net asset value (digits only)
                            ######### Please note that the N-Q values may be in 000's #########
                            net_assets_list = re.findall(r'\d+', next_td_text)
                            net_assets = "".join(net_assets_list)
                            return net_assets

                    except:
                        pass

            except:
                pass


        logging.info(f'''No net asset value found for {series_name} for document {adsh}''')


    def get_nq_series_data(self, series, xml, report_type):
        '''
        Gets data for a given series in an N-Q report
        @param series: series id
        @param xml: the content of N-Q report in xml format
        @report_type: N-Q or N-Q/A
        '''

        #Pre-allocate fields with NULL for database tables

        #date, quarter end date
        dates_data = [None, None]

        #adsh, report type, filing date, period end date, series id, net assets
        holdings_data = [None, None, None, None, None, None]

        adsh = self.get_adsh(xml)
        period_end_date = self.get_period_end_date(xml)
        quarter_end_date = self.translate_period_end_quarter_end(period_end_date)

        #Assign dates data
        dates_data[0] = period_end_date
        dates_data[1] = quarter_end_date

        #Assign holdings data
        holdings_data[0] = adsh
        holdings_data[1] = report_type
        holdings_data[2] = self.get_filed_date(xml)
        holdings_data[3] = period_end_date
        holdings_data[4] = series
        holdings_data[5] = self.get_nq_net_assets(series, xml, self.get_adsh(xml))

        #Convert to tuples
        dates_data = tuple(dates_data)
        holdings_data = tuple(holdings_data)

        return dates_data, holdings_data


    def get_nport_net_assets(self, xml):

        net_assets = xml.find('netassets').text

        return net_assets


    def get_nport_data(self, series, xml, report_type):

        '''
        Gets data for a given series in an NPORT-P report
        @param series: series id
        @param xml: the content of NPORT-P report in xml format
        @report_type: NPORT-P or NPORT-P/A
        '''

        #Pre-allocate fields with NULL for database tables

        #date, quarter end date
        dates_data = [None, None]

        #adsh, report type, filing date, period end date, series id, net assets
        holdings_data = [None, None, None, None, None, None]

        adsh = self.get_adsh(xml)
        period_end_date = self.get_period_end_date(xml)
        quarter_end_date = self.translate_period_end_quarter_end(period_end_date)

        #Assign dates data
        dates_data[0] = period_end_date
        dates_data[1] = quarter_end_date

        #Assign holdings data
        holdings_data[0] = adsh
        holdings_data[1] = report_type
        holdings_data[2] = self.get_filed_date(xml)
        holdings_data[3] = period_end_date
        holdings_data[4] = series
        holdings_data[5] = self.get_nport_net_assets(xml)

        #Convert to tuples
        dates_data = tuple(dates_data)
        holdings_data = tuple(holdings_data)

        return dates_data, holdings_data


    def obtain_insert_holdings_data(self, db_manager, proxy_manager):

        for index in self.filtered_report_urls:

            for report in index:

                #Get content from url
                try:
                    response = proxy_manager.session.get(report['url'], headers={'User-Agent': self.config['http_session']['user_agent']})
                except:
                    print(f"Did not receive response from SEC website for url {report['url']}. The site may be down; please check and re-run when it is available.")
                    logging.info(f"Did not receive response from SEC website for url {report['url']}. The site may be down; please check and re-run when it is available.")

                #Transfer content to xml format
                xml = BeautifulSoup(response.content, 'lxml')

                if ((report['filing_type'] == 'N-Q') | (report['filing_type'] == 'N-Q/A')):

                    series_list = self.get_series_in_report(xml)
                    filtered_series_list = self.filter_to_desired_series(series_list)

                    for series in filtered_series_list:

                        dates_data, holdings_data = self.get_nq_series_data(series, xml, report['filing_type'])

                        #insert or replace into dates
                        db_manager.insert_dates(dates_data)

                        #insert or replace into holdings
                        db_manager.insert_holdings(holdings_data)

                        logging.info(f'''Holdings data obtained and inserted for {series} {report['filing_type']} with filing period end date of {dates_data[0]}''')

                elif ((report['filing_type'] == 'NPORT-P') | (report['filing_type'] == 'NPORT-P/A')):

                    series_list = self.get_series_in_report(xml)
                    filtered_series_list = self.filter_to_desired_series(series_list)

                    #Don't need to loop through NPORT (because 1 series per report), but just easier to reuse the series list methods
                    for series in filtered_series_list:

                        dates_data, holdings_data = self.get_nport_data(series, xml, report['filing_type'])

                        #insert or replace into dates
                        db_manager.insert_dates(dates_data)

                        #insert or replace into holdings
                        db_manager.insert_holdings(holdings_data)

                        logging.info(f'''Holdings data obtained and inserted for {series} {report['filing_type']} with filing period end date of {dates_data[0]}''')


class databaseManager():
    '''Performs all database operations'''

    def __init__(self, config):

        self.conn = None
        self.cursor = None
        self.config = config


    def db_decorator(db_method):
        '''Decorator/wrapper for database CRUD operations; creates connection & cursor before operation, commits changes, and closes'''

        @functools.wraps(db_method)
        def db_wrapper(self, *args):

            database_filepath = self.config['network_drives']['database'] + '\\sec_extractor.db'
            self.conn = sqlite3.connect(database_filepath)
            self.cursor = self.conn.cursor()

            db_method_return = db_method(self, *args)

            self.conn.commit()
            self.cursor.close()
            self.conn.close()

            return db_method_return

        return db_wrapper


    @db_decorator
    def create_tables(self):

        create_tables = '''
        CREATE TABLE IF NOT EXISTS entities(
        CLASS_ID TEXT UNIQUE,
        SERIES_ID TEXT,
        CIK INTEGER,
        COMPANY TEXT);

        CREATE UNIQUE INDEX
            IF NOT EXISTS CLASS_ID_IDX
            ON entities(CLASS_ID);

        CREATE TABLE IF NOT EXISTS dates(
                DATE TEXT UNIQUE,
                QUARTER_END_DATE TEXT);

        CREATE UNIQUE INDEX
            IF NOT EXISTS DATE_IDX
            ON dates(DATE);

        CREATE TABLE IF NOT EXISTS prospectus(
            ADSH TEXT,
            FILING_TYPE TEXT,
            FILING_DATE TEXT,
            EFFECTIVE_DATE TEXT,
            CLASS_ID TEXT,
            EXPENSE_RATIO REAL,
            NET_EXPENSE_RATIO REAL,
            AVG_ANN_1YR_RETURN REAL,
            AVG_ANN_5YR_RETURN REAL,
            AVG_ANN_10YR_RETURN REAL,
            AVG_ANN_RETURN_SINCE_INCEPTION REAL,
            PRIMARY KEY (CLASS_ID, EFFECTIVE_DATE),
            FOREIGN KEY (CLASS_ID) REFERENCES entities(CLASS_ID),
            FOREIGN KEY (EFFECTIVE_DATE) REFERENCES dates(DATE));

        CREATE TABLE IF NOT EXISTS holdings(
            ADSH TEXT,
            FILING_TYPE TEXT,
            FILING_DATE TEXT,
            PERIOD_END_DATE TEXT,
            SERIES_ID TEXT,
            NET_ASSETS REAL,
            PRIMARY KEY (SERIES_ID, PERIOD_END_DATE),
            FOREIGN KEY (PERIOD_END_DATE) REFERENCES dates(DATE));

        CREATE TABLE IF NOT EXISTS quarters(
            QUARTER TEXT UNIQUE);
        '''

        self.cursor.executescript(create_tables)


    @db_decorator
    def insert_first_date(self):
        '''
        Insert first date with available & desired data (minus one day) if not already in dates table
        Required to get most recent date if no data input yet
        '''

        #Get day before configured start_year
        desired_start = dt.datetime(int(self.config['index']['start_year']), 1, 1)
        first_date = desired_start - dt.timedelta(1)

        #Format date to be inserted
        first_date_int_str = first_date.strftime(format='%Y%m%d')
        first_date_str = first_date.strftime(format='%Y-%m-%d')
        #quarter end date is same as first_date_str

        insert_first_date = '''
        INSERT OR REPLACE INTO dates (DATE, QUARTER_END_DATE) VALUES (?,?)
        '''
        self.cursor.execute(insert_first_date, (first_date_str, first_date_str,))


    @db_decorator
    def get_most_recent_holdings_date(self):

        max_date = '''
        SELECT MAX(date(FILING_DATE))
        FROM holdings
        '''
        self.cursor.execute(max_date)
        most_recent_date = self.cursor.fetchone()[0]
        most_recent_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d')

        logging.info(f'Most recent filing date in holdings table is {most_recent_date}')

        return most_recent_date


    @db_decorator
    def get_most_recent_prospectus_date(self):

        max_date = '''
        SELECT MAX(date(FILING_DATE))
        FROM prospectus
        '''
        self.cursor.execute(max_date)
        most_recent_date = self.cursor.fetchone()[0]
        most_recent_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d')

        logging.info(f'Most recent filing date in prospectus table is {most_recent_date}')

        return most_recent_date


    @db_decorator
    def get_most_recent_effective_date(self):

        max_date = '''
        SELECT MAX(date(EFFECTIVE_DATE))
        FROM prospectus
        '''
        self.cursor.execute(max_date)
        most_recent_date = self.cursor.fetchone()[0]
        most_recent_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d')

        logging.info(f'Most recent effective date in prospectus table is {most_recent_date}')

        return most_recent_date


    @db_decorator
    def get_least_recent_effective_date(self):

        min_date = '''
        SELECT MIN(date(EFFECTIVE_DATE))
        FROM prospectus
        '''
        self.cursor.execute(min_date)
        least_recent_date = self.cursor.fetchone()[0]
        least_recent_date = dt.datetime.strptime(least_recent_date, '%Y-%m-%d')

        logging.info(f'Least recent effective date in prospectus table is {least_recent_date}')

        return least_recent_date


    @db_decorator
    def get_most_recent_qtr_end_date(self):

        max_qtr_end_date = '''
        SELECT MAX(date(QUARTER_END_DATE))
        FROM dates
        '''
        self.cursor.execute(max_qtr_end_date)
        most_recent_qtr_end_date = self.cursor.fetchone()[0]
        most_recent_qtr_end_date = dt.datetime.strptime(most_recent_qtr_end_date, '%Y-%m-%d')

        logging.info(f'Most recent quarter end date in database is {most_recent_qtr_end_date}')

        return most_recent_qtr_end_date


    @db_decorator
    def insert_entities(self, entities_tuple):

        sql='''INSERT OR REPLACE INTO entities (CLASS_ID, SERIES_ID, CIK, COMPANY) VALUES (?,?,?,?)'''

        self.cursor.execute(sql, entities_tuple)


    @db_decorator
    def insert_dates(self, dates_tuple):

        sql='''INSERT OR REPLACE INTO dates (DATE, QUARTER_END_DATE) VALUES (?,?)'''

        self.cursor.execute(sql, dates_tuple)


    @db_decorator
    def insert_holdings(self, holdings_tuple):

        sql='''INSERT OR REPLACE INTO holdings (ADSH, FILING_TYPE, FILING_DATE, PERIOD_END_DATE, SERIES_ID, NET_ASSETS) VALUES (?,?,?,?,?,?)'''

        self.cursor.execute(sql, holdings_tuple)


    @db_decorator
    def insert_prospectuses(self, prospectuses_tuple):

        sql='''INSERT OR REPLACE INTO prospectus (ADSH, FILING_TYPE, FILING_DATE, EFFECTIVE_DATE, CLASS_ID, EXPENSE_RATIO, NET_EXPENSE_RATIO, AVG_ANN_1YR_RETURN, AVG_ANN_5YR_RETURN, AVG_ANN_10YR_RETURN, AVG_ANN_RETURN_SINCE_INCEPTION) VALUES (?,?,?,?,?,?,?,?,?,?,?)'''

        self.cursor.execute(sql, prospectuses_tuple)


    @db_decorator
    def insert_quarters(self, quarters_tuple):

        sql='''INSERT OR REPLACE INTO quarters (QUARTER) VALUES (?)'''

        self.cursor.execute(sql, quarters_tuple)


    @db_decorator
    def select_data(self):

        query = '''
        SELECT hold.FILING_TYPE AS HOLDINGS_FILING_TYPE, pros.FILING_TYPE AS PROSPECTUS_FILING_TYPE, pros.CIK_IMPUTE, pros.COMPANY_IMPUTE, hold.SERIES_ID, hold.QUARTER_END_DATE, AVERAGE_NET_ASSETS, pros.avgexpratio AS AVERAGE_EXPENSE_RATIO, pros.AVERAGE_EXPENSE_RATIO_IMPUTE, pros.avgnetexpratio AS AVERAGE_NET_EXPENSE_RATIO, pros.AVERAGE_NET_EXPENSE_RATIO_IMPUTE, pros.AVERAGE_ANNUAL_1YR_RETURN, pros.AVERAGE_ANNUAL_5YR_RETURN, pros.AVERAGE_ANNUAL_10YR_RETURN, pros.AVERAGE_ANNUAL_RETURN_SINCE_INCEPTION
        FROM
        (SELECT h.SERIES_ID, h.FILING_TYPE, dates.QUARTER_END_DATE, AVG(NET_ASSETS) AVERAGE_NET_ASSETS FROM holdings h
        LEFT JOIN dates ON date(h.PERIOD_END_DATE) = date(dates.DATE) GROUP BY h.SERIES_ID, dates.QUARTER_END_DATE) hold
        INNER JOIN
            (SELECT all_qtrs.SERIES_ID, avg_er.CIK, avg_er.COMPANY, avg_er.AVERAGE_ANNUAL_1YR_RETURN, avg_er.AVERAGE_ANNUAL_5YR_RETURN, avg_er.AVERAGE_ANNUAL_10YR_RETURN, avg_er.AVERAGE_ANNUAL_RETURN_SINCE_INCEPTION, avg_er.FILING_TYPE, all_qtrs.QUARTER, avg_er.avgexpratio, COALESCE(avg_er.avgexpratio,
                (SELECT avg_er2.avgexpratio FROM
                    (SELECT e.SERIES_ID, AVG(p2.EXPENSE_RATIO) AS avgexpratio, d.QUARTER_END_DATE FROM prospectus p2
                    INNER JOIN dates d ON date(p2.EFFECTIVE_DATE) = date(d.DATE)
                    INNER JOIN entities e ON p2.CLASS_ID = e.CLASS_ID
                    GROUP BY e.SERIES_ID, d.QUARTER_END_DATE
                    ORDER BY e.SERIES_ID, d.DATE
                    ) avg_er2
                WHERE all_qtrs.SERIES_ID = avg_er2.SERIES_ID AND date(avg_er2.QUARTER_END_DATE) < date(all_qtrs.QUARTER)
                ORDER BY date(avg_er2.QUARTER_END_DATE) DESC LIMIT 1)) AVERAGE_EXPENSE_RATIO_IMPUTE,
                avg_er.avgnetexpratio,
                COALESCE(avg_er.avgnetexpratio,
                (SELECT avg_er2.avgnetexpratio FROM
                    (SELECT e.SERIES_ID, AVG(p2.NET_EXPENSE_RATIO) AS avgnetexpratio, d.QUARTER_END_DATE FROM prospectus p2
                    INNER JOIN dates d ON date(p2.EFFECTIVE_DATE) = date(d.DATE)
                    INNER JOIN entities e ON p2.CLASS_ID = e.CLASS_ID
                    GROUP BY e.SERIES_ID, d.QUARTER_END_DATE
                    ORDER BY e.SERIES_ID, d.DATE
                    ) avg_er2
                WHERE all_qtrs.SERIES_ID = avg_er2.SERIES_ID AND date(avg_er2.QUARTER_END_DATE) < date(all_qtrs.QUARTER)
                ORDER BY date(avg_er2.QUARTER_END_DATE) DESC LIMIT 1)) AVERAGE_NET_EXPENSE_RATIO_IMPUTE,
                COALESCE(avg_er.CIK,
                    (SELECT avg_er2.avgcik FROM
                        (SELECT e.SERIES_ID, e.CIK AS avgcik, d.QUARTER_END_DATE FROM prospectus p2
                        INNER JOIN dates d ON date(p2.EFFECTIVE_DATE) = date(d.DATE)
                        INNER JOIN entities e ON p2.CLASS_ID = e.CLASS_ID
                        GROUP BY e.SERIES_ID, d.QUARTER_END_DATE
                        ORDER BY e.SERIES_ID, d.DATE
                        ) avg_er2
                    WHERE all_qtrs.SERIES_ID = avg_er2.SERIES_ID AND date(avg_er2.QUARTER_END_DATE) < date(all_qtrs.QUARTER)
                    ORDER BY date(avg_er2.QUARTER_END_DATE) DESC LIMIT 1)) CIK_IMPUTE,
                COALESCE(avg_er.COMPANY,
                    (SELECT avg_er2.avgcompany FROM
                        (SELECT e.SERIES_ID, e.COMPANY AS avgcompany, d.QUARTER_END_DATE FROM prospectus p2
                        INNER JOIN dates d ON date(p2.EFFECTIVE_DATE) = date(d.DATE)
                        INNER JOIN entities e ON p2.CLASS_ID = e.CLASS_ID
                        GROUP BY e.SERIES_ID, d.QUARTER_END_DATE
                        ORDER BY e.SERIES_ID, d.DATE
                        ) avg_er2
                    WHERE all_qtrs.SERIES_ID = avg_er2.SERIES_ID AND date(avg_er2.QUARTER_END_DATE) < date(all_qtrs.QUARTER)
                    ORDER BY date(avg_er2.QUARTER_END_DATE) DESC LIMIT 1)) COMPANY_IMPUTE
            FROM
                (SELECT SERIES_ID, QUARTER FROM
                    (SELECT SERIES_ID, MIN(QUARTER_END_DATE) min_qtr_date, MAX(QUARTER_END_DATE) max_qtr_date
                    FROM prospectus p1
                    INNER JOIN dates d1 ON date(p1.EFFECTIVE_DATE) = date(d1.DATE)
                    INNER JOIN entities e1 ON p1.CLASS_ID = e1.CLASS_ID
                    GROUP BY e1.SERIES_ID
                    ) min_max
                CROSS JOIN quarters q2 WHERE date(q2.QUARTER) BETWEEN min_qtr_date AND max_qtr_date
            ) all_qtrs
            LEFT JOIN
                (SELECT e.SERIES_ID, e.CIK, e.COMPANY, p2.FILING_TYPE, AVG(AVG_ANN_1YR_RETURN) AS AVERAGE_ANNUAL_1YR_RETURN, AVG(AVG_ANN_5YR_RETURN) AS AVERAGE_ANNUAL_5YR_RETURN, AVG(AVG_ANN_10YR_RETURN) AS AVERAGE_ANNUAL_10YR_RETURN, AVG(AVG_ANN_RETURN_SINCE_INCEPTION) AS AVERAGE_ANNUAL_RETURN_SINCE_INCEPTION, AVG(p2.NET_EXPENSE_RATIO) as avgnetexpratio, AVG(p2.EXPENSE_RATIO) AS avgexpratio, d.QUARTER_END_DATE FROM prospectus p2
                INNER JOIN dates d ON date(p2.EFFECTIVE_DATE) = date(d.DATE)
                INNER JOIN entities e ON p2.CLASS_ID = e.CLASS_ID
                GROUP BY e.SERIES_ID, d.QUARTER_END_DATE
                ORDER BY e.SERIES_ID, d.DATE
                ) avg_er
            ON all_qtrs.SERIES_ID = avg_er.SERIES_ID AND date(all_qtrs.QUARTER) = date(avg_er.QUARTER_END_DATE)
            ORDER BY all_qtrs.SERIES_ID, date(all_qtrs.QUARTER)) pros
        ON hold.SERIES_ID = pros.SERIES_ID AND date(hold.QUARTER_END_DATE) = date(pros.QUARTER)
        ORDER BY hold.SERIES_ID, hold.QUARTER_END_DATE
        '''

        df = pd.read_sql(query, self.conn)
        print(df)
        df.to_csv('sec_extractor.csv', index=False)


class prospectusCourier():


    def __init__(self, config, db_manager):

        self.db_manager = db_manager
        self.base_url = 'https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets/'
        self.end_url = '_rr1.zip'
        self.config = config
        self.quarter_end_dates = None
        self.quarter_list = None
        self.url_file_list = None
        self.zip_files = None
        self.filtered_zip_files = None
        self.prospectus_paths = None


    def get_list_quarters(self):

        self.quarter_list = []

        self.get_list_quarters_dates()

        for quarter_end in self.quarter_end_dates:

            self.quarter_list.append(self.translate_quarter_end_to_quarter(quarter_end))


    def get_list_quarters_dates(self, start=None, end=None):
        '''Returns list of quarter end dates (in string %Y-%m-%d format) between two dates'''

        if start is None:
            start = str(self.config['prospectus']['start_year']) + '-01-01'

        if end is None:
            end = dt.datetime.today().strftime('%Y-%m-%d')

        self.quarter_end_dates = pd.date_range(pd.to_datetime(start), pd.to_datetime(end) + pd.offsets.QuarterBegin(1), freq='Q').strftime('%Y-%m-%d').tolist()


    def translate_quarter_end_to_quarter(self, date):
        '''Returns yyyyqx for a given string date in %Y-%m-%d format'''

        month = date[5:7]

        if month in ['01', '02', '03']:
            return date[0:4] + 'q1'
        elif month in ['04', '05', '06']:
            return date[0:4] + 'q2'
        elif month in ['07', '08', '09']:
            return date[0:4] + 'q3'
        elif month in ['10', '11', '12']:
            return date[0:4] + 'q4'


    def get_list_url_files(self):
        '''Gets list of prospectus data zip files to be downloaded from SEC'''

        self.url_file_list = []

        for quarter in self.quarter_list:

            file = self.base_url + quarter + self.end_url

            self.url_file_list.append(file)


    def download_zip_files(self):
        '''Download prospectus zip files from SEC urls'''

        folder_path = self.config['network_drives']['zip_prospectuses']

        start_time = time.time()

        for i,url in enumerate(self.url_file_list):

            file_path = folder_path + '\\' + self.quarter_end_dates[i] + '.zip'

            try:

                if not os.path.exists(file_path):

                    urllib.request.urlretrieve(url, file_path)

                    logging.info(f'''Downloaded {file_path}''')

            except:
                logging.info(f'Could not download {url}. It may not yet exist.')

        time_taken = (time.time() - start_time)/60
        logging.info(f'''Prospectus zip files download took {time_taken:.2f} minutes''')


    def translate_zip_to_date(self, zip_path):
        '''Gets zip file stem (quarter end date for zip file)'''

        return Path(zip_path).stem


    def filter_zip_files(self):
        '''Gets prospectus zip files that have yet to be inserted into database'''

        glob_str = self.config['network_drives']['zip_prospectuses'] + '\\*.zip'
        self.zip_files = sorted(glob.glob(glob_str))

        self.filtered_zip_files = []

        for zip in self.zip_files:

            file_stem = self.translate_zip_to_date(zip)
            file_date = dt.datetime.strptime(file_stem, '%Y-%m-%d')

            try:
                most_recent_date = self.db_manager.get_most_recent_prospectus_date()
            except:
                most_recent_date = dt.datetime(int(self.config['prospectus']['start_year']), 1, 1)

            if file_date >= most_recent_date:

                self.filtered_zip_files.append(zip)

        logging.info(f'''Prospectus zip files to be included in database insert: {self.filtered_zip_files}''')


    def extract_zip_content(self, filepath, extract_file_name: str, save_unzipped_folder: str, exact: bool = False):
        '''
        Unzips zip file
        @param extract_file_name: file (file name, and file type must be included) to be extracted from zip
        @param save_unzipped_folder: folderpath (entire folderpath (do NOT include file name or type) must be included) where the unzipped attachment will be saved
        @param exact: If False, will extract file from zip that has extract_file_name in its name. If True, will only extract file from zip with exact string as extract_file_name.
        '''

        #type checking
        assert(isinstance(exact, bool)), f'''exact must be of type bool. You input an object of type {type(exact)}.'''
        assert(isinstance(extract_file_name, str)), f'''extract_file_name must be of type string. You input an object of type {type(extract_file_name)}.'''
        assert(isinstance(save_unzipped_folder, str)), f'''save_unzipped_folder must be of type string. You input an object of type {type(save_unzipped_folder)}.'''

        if not (zipfile.is_zipfile(filepath)):
            logging.info(f'''{filepath} is not a zip file; therefore it cannot be unzipped.''')
        else:
            #Create zipfile object with positions zip
            zip_file = zipfile.ZipFile(filepath)

            #Save specified file within zip
            file_found = False
            for file_name in zip_file.namelist():

                if exact is True:

                    if extract_file_name == file_name:

                        zip_file.extract(file_name, save_unzipped_folder)
                        filepath = str(save_unzipped_folder) + "\\" + file_name
                        file_found = True
                        logging.info(f'''Unzipped attachment saved as {filepath}''')

                elif exact is False:

                    if extract_file_name in file_name:

                        zip_file.extract(file_name, save_unzipped_folder)
                        filepath = str(save_unzipped_folder) + "\\" + file_name

                        file_found = True
                        logging.info(f'''Unzipped attachment saved as {filepath}''')

            if file_found is False:
                logging.info(f'''Could not find file {file_name} in zipfile.''')


    def get_quarter_prospectuses(self):

        folder_path = self.config['network_drives']['prospectuses']
        self.prospectus_paths = []

        start_time = time.time()

        for i,zip in enumerate(self.filtered_zip_files):

            #Make new folder for sub and num files
            prospectus_path = folder_path + r'\\' + Path(zip).stem
            os.makedirs(prospectus_path, exist_ok=True)
            self.prospectus_paths.append(prospectus_path)

            #Extract sub file
            self.extract_zip_content(zip, 'sub.tsv', prospectus_path, exact=True)
            #Extract num file
            self.extract_zip_content(zip, 'num.tsv', prospectus_path, exact=True)

        time_taken = (time.time() - start_time)/60
        logging.info(f'''Unzipping prospectus files took {time_taken:.2f} minutes''')


    def read_sub(self, prospectus_quarter):
        '''Reads sub.tsv into dataframe'''

        #Parameters for reading in prospectus files
        use_columns = ['adsh', 'cik', 'name', 'effdate', 'filed', 'form']
        dtype_dict = {'adsh': str, 'cik': str, 'name': str, 'form': str}

        #File
        sub = prospectus_quarter + r'\\' + 'sub.tsv'

        chunks = []
        #Read data in chunks of 100,000 to ensure enough memory
        chunk_index = 0
        for chunk in pd.read_csv(sub, sep='\t', usecols=use_columns, dtype=dtype_dict, parse_dates=['effdate', 'filed'], infer_datetime_format=True, engine='python', chunksize=100000, quoting=3):
            chunk_index += 1
            logging.info(f'''Read in chunk {chunk_index} of 100,000 rows of data for {sub}''')
            chunks.append(chunk)

        #Concatenate chunks of rows into single dataframe with all trades data
        sub_df = pd.concat(chunks, ignore_index=True)

        return sub_df


    def read_num(self, prospectus_quarter):
        '''Reads num.tsv into dataframe'''

        #Parameters for reading in prospectus files
        use_columns = ['adsh', 'tag', 'series', 'class', 'value']
        dtype_dict = {'adsh': str, 'tag': str, 'series': str, 'class': str, 'value': float}

        #File
        num = prospectus_quarter + r'\\' + 'num.tsv'

        chunks = []
        #Read data in chunks of 100,000 to ensure enough memory
        chunk_index = 0
        for chunk in pd.read_csv(num, sep='\t', usecols=use_columns, dtype=dtype_dict, engine='python', chunksize=100000, quoting=3):
            chunk_index += 1
            logging.info(f'''Read in chunk {chunk_index} of 100,000 rows of data for {num}''')
            chunks.append(chunk)

        #Concatenate chunks of rows into single dataframe with all trades data
        num_df = pd.concat(chunks, ignore_index=True)

        return num_df


    def pre_join_sub_filter(self, sub_df):
        '''effdate is a primary key in the database, so must take out all rows with empty effdate cell'''

        sub_df = sub_df.dropna(subset=['effdate'])

        return sub_df


    def pre_join_num_filter(self, num_df):
        '''Filter num dataframe to only include desired series id's and desired tags and non-NA values'''

        desired_tags = ["ExpensesOverAssets", "NetExpensesOverAssets", "AverageAnnualReturnYear01", "AverageAnnualReturnYear05", "AverageAnnualReturnYear10", "AverageAnnualReturnSinceInception"]

        num_df = num_df[num_df['series'].isin(self.config['series_to_index'].keys())]
        num_df = num_df[num_df['tag'].isin(desired_tags)]
        num_df = num_df.dropna(subset=['class'])

        return num_df


    def join_quarter_prospectuses_files(self, prospectus_quarter):
        '''Inner join num and sub dataframes'''

        #Read in datasets
        sub_df = self.read_sub(prospectus_quarter)
        sub_df = self.pre_join_sub_filter(sub_df)
        num_df = self.read_num(prospectus_quarter)
        num_df = self.pre_join_num_filter(num_df)

        #Inner join dataframes
        df = num_df.merge(sub_df, how='inner')

        return df


    def pivot_quarter_prospectuses(self, df):
        '''Pivot dataframe such that each set of indexes will have one row, and columns for desired tags are added, and values are averaged if multiple rows with identical sets of indexes'''

        pivot_df = pd.pivot_table(df, values='value', index=['adsh', 'class', 'series', 'cik', 'name', 'effdate', 'filed', 'form'], columns=['tag'], aggfunc=np.mean)

        return pivot_df


    def get_prospectuses_data(self, prospectus_quarter):

        df = self.join_quarter_prospectuses_files(prospectus_quarter)
        pivot_df = self.pivot_quarter_prospectuses(df)

        #Form back into dataframe (rid multi-index)
        pivot_df = pivot_df.rename_axis(None, axis=1).reset_index()

        return pivot_df


    def translate_period_end_quarter_end(self, period_end_date):
        '''Produces the quarter end date (yyyy-mm-dd) from any date (also needs to be formatted yyyy-mm-dd)'''

        month = period_end_date[5:7]

        if month in ['01', '02', '03']:
            return period_end_date[0:4] + '-' + '03' + '-' + '31'
        elif month in ['04', '05', '06']:
            return period_end_date[0:4] + '-' + '06' + '-' + '30'
        elif month in ['07', '08', '09']:
            return period_end_date[0:4] + '-' + '09' + '-' + '30'
        elif month in ['10', '11', '12']:
            return period_end_date[0:4] + '-' + '12' + '-' + '31'


    def get_dates_table_data(self, df):
        '''Gets list of tuples of dates to be input into database'''

        dates_df = df[['effdate']]

        #Create desired fields
        dates_df['EFFECTIVE_DATE'] = dates_df['effdate'].apply(lambda date: date.strftime('%Y-%m-%d'))
        dates_df['QUARTER_END_DATE'] = dates_df['EFFECTIVE_DATE'].apply(lambda date: self.translate_period_end_quarter_end(date))
        dates_df = dates_df[['EFFECTIVE_DATE', 'QUARTER_END_DATE']]

        dates = list(dates_df.to_records(index=False))

        return dates


    def insert_dates_list(self, dates):
        '''Inserts list of dates tuples into databse'''

        for date_tuple in dates:

            self.db_manager.insert_dates(date_tuple)


    def get_entities_table_data(self, df):

        entities_df = df[['class', 'series', 'cik', 'name']]

        entities = list(entities_df.to_records(index=False))

        return entities


    def insert_entities_list(self, entities):

        for entitity_tuple in entities:

            self.db_manager.insert_entities(entitity_tuple)


    def get_prospectus_table_data(self, df):

        #Add columns if they don't exist
        pivot_cols = ['ExpensesOverAssets', 'NetExpensesOverAssets', 'AverageAnnualReturnYear01', 'AverageAnnualReturnYear05', 'AverageAnnualReturnYear10', 'AverageAnnualReturnSinceInception']
        df_cols = list(df.columns)

        for col in pivot_cols:

            if col not in df_cols:

                df[col] = np.nan

        pros_df = df[['adsh', 'form', 'filed', 'effdate', 'class', 'ExpensesOverAssets', 'NetExpensesOverAssets', 'AverageAnnualReturnYear01', 'AverageAnnualReturnYear05', 'AverageAnnualReturnYear10', 'AverageAnnualReturnSinceInception']]

        #Datetime columns to string
        pros_df['filed'] = pros_df['filed'].apply(lambda date: date.strftime('%Y-%m-%d'))
        pros_df['effdate'] = pros_df['effdate'].apply(lambda date: date.strftime('%Y-%m-%d'))

        prospectuses = list(pros_df.to_records(index=False))

        return prospectuses


    def insert_prospectuses_list(self, prospectuses):

        for prospectus_tuple in prospectuses:

            self.db_manager.insert_prospectuses(prospectus_tuple)


    def get_quarters_table_data(self):
        '''Gets all quarter end dates between minimum effective date in prospectus table and maximum effective date; used for imputing expense ratios'''

        min_date = self.db_manager.get_least_recent_effective_date()
        max_date = self.db_manager.get_most_recent_effective_date()

        quarter_list = pd.date_range(pd.to_datetime(min_date), pd.to_datetime(max_date) + pd.offsets.QuarterBegin(1), freq='Q').strftime('%Y-%m-%d').tolist()

        #Convert to list of tuples
        return [(quarter,) for quarter in quarter_list]


    def insert_quarters_list(self, quarters):

        for quarter_tuple in quarters:

            self.db_manager.insert_quarters(quarter_tuple)


    def obtain_insert_prospectus_data(self):
        '''Obtains prospectus data from datasets using help methods; inserts data into database'''

        start_time = time.time()

        for i,prospectus_quarter in enumerate(self.prospectus_paths):

            pivot_df = self.get_prospectuses_data(prospectus_quarter)

            logging.info(f'''Prospectuses data read, filtered, joined, and pivoted for {prospectus_quarter}''')

            if len(pivot_df) == 0:

                logging.info(f'''Prospectuses data is empty for {prospectus_quarter}''')
                continue

            dates = self.get_dates_table_data(pivot_df)

            self.insert_dates_list(dates)

            logging.info(f'''Dates data inserted for {prospectus_quarter}''')

            entities = self.get_entities_table_data(pivot_df)

            self.insert_entities_list(entities)

            logging.info(f'''Entities data inserted for {prospectus_quarter}''')

            prospectuses = self.get_prospectus_table_data(pivot_df)

            self.insert_prospectuses_list(prospectuses)

            logging.info(f'''Prospectus data inserted for {prospectus_quarter}''')

            quarters = self.get_quarters_table_data()

            self.insert_quarters_list(quarters)

            logging.info(f'''Quarters data inserted for {prospectus_quarter}''')


if __name__ == "__main__":


    #Import configuration json file
    configuration_manager = configurationManager()
    config = configuration_manager.get_config()

    #Configure logging
    log_manager = logManager(config)
    log_manager.config_log()
    logging.info('###### sec_extractor.py has begun execution. Configuration file has been loaded without error. ######')
    log_manager.declare_computer_user()

    start_time = time.time()

    #Create HTTP(S) session with specified proxy
    proxy_manager = proxyManager()
    session = proxy_manager.set_http_session(config)

    #Get latest SEC index files
    index_courier = indexCourier(config)
    index_courier.obtain_index_files()

    #Create database
    db_manager = databaseManager(config)
    db_manager.create_tables()
    db_manager.insert_first_date()

    #Holdings courier
    holdings_courier = holdingsCourier(config)
    holdings_courier.filter_indexes(db_manager, index_courier)
    holdings_courier.get_report_urls()
    holdings_courier.obtain_insert_holdings_data(db_manager, proxy_manager)

    #Prospectus courier
    prospectus_courier = prospectusCourier(config, db_manager)
    prospectus_courier.get_list_quarters()
    prospectus_courier.get_list_url_files()
    prospectus_courier.download_zip_files()
    prospectus_courier.filter_zip_files()
    prospectus_courier.get_quarter_prospectuses()
    prospectus_courier.obtain_insert_prospectus_data()

    #Query database
    db_manager.select_data()

    time_taken = (time.time() - start_time)/60
    logging.info(f'''##### Application complete. It took {time_taken:.2f} minutes to execute. #####''')









