import unittest
from unittest.mock import MagicMock
from unittest import mock
import sec_extractor
import sqlite3
import os


def remove_database():
    '''Will delete test database, if it exists in expected directory'''

    try:
        os.remove('test_assets\\test_query\\test.db')
    except:
        pass


def create_test_tables(db):

    create_tables = '''
    CREATE TABLE IF NOT EXISTS entities(
    CLASS_ID TEXT UNIQUE,
    CLASS TEXT UNIQUE,
    SERIES_ID TEXT,
    SERIES TEXT,
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
        CLASS_ID TEXT,
        EXPENSE_RATIO REAL,
        EFFECTIVE_DATE TEXT,
        PRIMARY KEY (CLASS_ID, EFFECTIVE_DATE),
        FOREIGN KEY (CLASS_ID) REFERENCES entities(CLASS_ID),
        FOREIGN KEY (EFFECTIVE_DATE) REFERENCES dates(DATE));

    CREATE TABLE IF NOT EXISTS holdings(
        SERIES_ID TEXT,
        ASSETS REAL,
        PERIOD_END_DATE TEXT,
        PRIMARY KEY (SERIES_ID, PERIOD_END_DATE),
        FOREIGN KEY (PERIOD_END_DATE) REFERENCES dates(DATE));

    CREATE TABLE IF NOT EXISTS quarters(
        QUARTER TEXT UNIQUE);
    '''

    conn = sqlite3.connect(db)
    conn.cursor().executescript(create_tables)
    conn.commit()
    conn.close()


def populate_test_tables(db):
    '''Insert (or replace) data to be used in unit testing of database'''

    sql='''
    INSERT OR REPLACE INTO entities
        (CLASS_ID, CLASS, SERIES_ID, SERIES, CIK, COMPANY)
    VALUES (?,?,?,?,?,?)'''
    data=	(('a', 'A', 'a', None, None,None,),
        ('b', 'B', 'b', None,None,None,),
        ('c', 'Admiral', 'c', None,None,None,),
        ('d', 'Investor', 'd', None,None,None,),)

    conn = sqlite3.connect(db)
    conn.cursor().executemany(sql,data)
    conn.commit()
    conn.close()

    sql='''
    INSERT OR REPLACE INTO dates
        (DATE, QUARTER_END_DATE)
    VALUES (?,?)'''
    data=(('2021-03-30', '2021-03-31'),
        ('2021-06-30', '2021-06-30'),
        ('2020-01-01', '2020-03-31'),
        ('2021-01-01', '2021-03-31'),
        ('2020-12-30', '2020-12-31'),
        ('2021-03-30', '2021-03-31'),
        ('2021-04-01', '2021-06-30'),
        ('2021-04-01', '2021-06-30'),
        ('2021-04-04', '2021-06-30'),
        ('2021-08-01', '2021-09-30'),
        ('2020-01-15', '2020-03-31'),
        ('2020-04-15', '2020-06-30'),
        ('2020-07-15', '2020-09-30'),
        ('2020-10-15', '2020-12-31'),
        ('2021-01-15', '2021-03-31'),
        ('2020-12-31', '2020-12-31'),
        ('2021-04-01', '2021-06-30'),
        ('2021-04-05', '2021-06-30'),
        ('2021-06-30', '2021-06-30'),
        ('2021-08-01', '2021-09-30'),)

    conn = sqlite3.connect(db)
    conn.cursor().executemany(sql,data)
    conn.commit()
    conn.close()

    sql='''
    INSERT OR REPLACE INTO prospectus
        (CLASS_ID, EXPENSE_RATIO, EFFECTIVE_DATE)
    VALUES (?,?,?)'''
    data=(('a', .01, '2021-03-30'),
        ('b', .01, '2021-06-30'),
        ('c', .02, '2020-01-01'),
        ('d', .07, '2021-01-01'),
        ('e', .015, '2020-12-30'),
        ('f', .08, '2021-03-30'),
        ('g', .02, '2021-04-01'),
        ('h', .07, '2021-04-01'),
        ('i', .03, '2021-04-04'),
        ('j', .01, '2021-08-01'),)

    conn = sqlite3.connect(db)
    conn.cursor().executemany(sql, data)
    conn.commit()
    conn.close()

    sql='''
    INSERT OR REPLACE INTO holdings
        (SERIES_ID, ASSETS, PERIOD_END_DATE)
    VALUES (?,?,?)'''
    data=(('a', 50, '2020-01-15'),
        ('b', 57, '2020-04-15'),
        ('c', 60, '2020-07-15'),
        ('d', 65, '2020-10-15'),
        ('e', 62, '2021-01-15'),
        ('f', 100, '2020-12-31'),
        ('g', 120, '2021-04-01'),
        ('h', 123, '2021-04-05'),
        ('i', 120, '2021-06-30'),
        ('j', 127, '2021-08-01'),)

    conn = sqlite3.connect(db)
    conn.cursor().executemany(sql, data)
    conn.commit()
    conn.close()

    sql='''
    INSERT OR REPLACE INTO quarters (QUARTER) VALUES (?)'''
    data=(('2020-03-31',),
    ('2020-06-30',),
    ('2020-09-30',),
    ('2020-12-31',),
    ('2021-03-31',),
    ('2021-06-30',),
    ('2021-09-30',),
    ('2021-03-31',))

    conn = sqlite3.connect(db)
    conn.cursor().executemany(sql,data)
    conn.commit()
    conn.close()


class testDatabaseManager(unittest.TestCase):
    '''Tests the final query of the fund database'''

    def test_query(self):

        remove_database()
        create_test_tables('test_assets\\test_query\\test.db')
        populate_test_tables('test_assets\\test_query\\test.db')

        output =  [
        ('a', '2020-03-31', .02, 50.0),
        ('b', '2020-06-30',  .02, 57.0),
        ('c', '2020-09-30',  .02, 60.0),
        ('d', '2020-12-31',  .02, 65.0),
        ('e', '2021-03-31',  .04, 62.0),
        ('f', '2020-12-31',  .015, 100.0),
        ('g', '2021-06-30',  .04, 121.0),
        ('h', '2021-09-30',  .01, 127.0)]

        query = '''
        SELECT hold.SERIES_ID, hold.QUARTER_END_DATE, pros.AVERAGE_EXPENSE_RATIO, avg_assets
        FROM
        (SELECT h.SERIES_ID, dates.QUARTER_END_DATE, AVG(ASSETS) avg_assets FROM holdings h
        LEFT JOIN dates ON date(h.PERIOD_END_DATE) = date(dates.DATE) GROUP BY h.SERIES_ID, dates.QUARTER_END_DATE) hold
        LEFT JOIN
            (SELECT all_qtrs.SERIES_ID, all_qtrs.QUARTER, COALESCE(avg_er.avgexpratio,
                (SELECT avg_er2.avgexpratio FROM
                    (SELECT e.SERIES_ID, AVG(p2.EXPENSE_RATIO) AS avgexpratio, d.QUARTER_END_DATE FROM prospectus p2
                    INNER JOIN dates d ON date(p2.EFFECTIVE_DATE) = date(d.DATE)
                    INNER JOIN entities e ON p2.CLASS_ID = e.CLASS_ID
                    GROUP BY e.SERIES_ID, d.QUARTER_END_DATE
                    ORDER BY e.SERIES_ID, d.DATE
                    ) avg_er2
                WHERE all_qtrs.SERIES_ID = avg_er2.SERIES_ID AND date(avg_er2.QUARTER_END_DATE) < date(all_qtrs.QUARTER)
                ORDER BY date(avg_er2.QUARTER_END_DATE) DESC LIMIT 1)) AVERAGE_EXPENSE_RATIO
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
                (SELECT e.SERIES_ID, AVG(p2.EXPENSE_RATIO) AS avgexpratio, d.QUARTER_END_DATE FROM prospectus p2
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

        conn = sqlite3.connect('test_assets\\test_query\\test.db')
        cursor = conn.cursor()
        cursor.execute(query)

        query_list = []
        for row in cursor:
            query_list.append(row)

        conn.commit()
        cursor.close()
        conn.close()

        self.assertCountEqual(output, query_list)


if __name__ == "__main__":
    unittest.main()