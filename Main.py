#from input import *
import glob, os
import pandas as pd
import numpy as np
import datetime as dt
import requests
import io
import xml.etree.ElementTree as ET


file_types =  ['SEM02', 'SEM03', 'SEM21']
repo_trade_types = 'RHJLM'
ids = ['BoardId', 'SecurityId']
sem21_fields = ['Volume', 'OpenPeriod',
                'Open', 'Low', 'High', 'Close',
                'LowOffer', 'HighBid',
                'WAPrice', 'TrendClose', 'TrendWAP',
                'Bid', 'Offer', 'Prev', 'MarketPrice',
                'TrendClsPr', 'TrendWapPr',
                'MarketPrice2', 'MarketPrice3', 'PrevLegalClosePrice', 'LegalClosePrice',
                'MPValTrd', 'MP2ValTrd', 'MP3ValTrd',
                'Duration']
coeff_feilds = ['liquidity', 'sigma', 'beta', 'f_plus', 'f_minus', 'spread', 'coeff_c']

class XML2DataFrame:

    def __init__(self, xml_data):
        self.root = ET.XML(xml_data)

    def parse_element(self, element):
        #results = {}
        if element.tag == 'SEM21':
            #results['SEM21']
            return self.parse_middle(element)
        if element.tag == 'SEM02':
            return self.parse_middle(element)
        if element.tag == 'SEM03':
            return self.parse_middle(element)

        for child in list(element):
            child_result = self.parse_element(child)
            if child_result is not None:
                return child_result
                #if tag not in results.keys():
                #    results[tag] = []
                #results[tag].append(child_result)

        return None

    def parse_middle(self, element):
        boards = []
        records = []
        for child in list(element):
            if child.tag == 'RECORDS':
                record = self.parse_record(child)
                if record is not None:
                    records.append(record)
            else:
                board = self.parse_middle(child)
                if board is not None:
                    boards.append(board)

        if len(records) > 0:
            boards.append(pd.DataFrame(records))

        if len(boards) > 0:
            result = pd.concat(boards)
            for key in element.keys():
                result[key] = element.attrib.get(key)
            return result

        return None

    @staticmethod
    def parse_record(record):
        result = {}
        for key in record.keys():
            result[key] = record.attrib.get(key)
        return result

    def process_data(self):
        """ Initiate the root XML, parse it, and return a dataframe"""
        boards = list(filter(lambda board: board is not None, [self.parse_element(child) for child in list(self.root)]))
        return pd.concat(boards) if len(boards) > 0 else None

    def process_pricees(self):
        rows  = [self.parse_record(row) for row in  filter(lambda node: node.tag == "row", list(self.root))]
        if rows:
            return pd.DataFrame(rows)
        else:
            return None


def get_prices(dates, path):
    all_prices = []
    price_files = list(filter(lambda x: 'stock_current_price_' in x, os.listdir(path)))

    for date in dates:
        date_str = date.replace('-', '')
        date_files = list(filter(lambda file: date_str in file, price_files))
        prices = None
        if len(date_files)>0:
            date_file = date_files[0]
            if '.xml' in date_file:
                with open(date_files[0]) as f:
                    xml2df = XML2DataFrame(f.read())
                    prices = xml2df.process_pricees()
                    if prices is not None:
                        prices['TRADEDATE'] = date
                        all_prices.append(prices)
                    else:
                        print('No prices data was found in ' + date_file)
            elif '.csv' in date_file:
                prices = pd.read_csv(date_file, sep=';')

        if prices is None:
            print('No price files were found for the date: ' + date + '. Trying to get from the server.')
            prices = None
        #     url = 'https://iss.moex.com/issrpc/marketdata/stock/current_price/stock_current_price_' + date_str + '.csv&tradedate=' + date
        #         #'https://iss.moex.com/issrpc/marketdata/stock/current_price/stock_current_price_20170320.csv?tradedate=2017-03-20'
        #     s = requests.get(url)
        #     string_io = io.StringIO(s.content.decode('utf-8'))
        #     prices = pd.read_csv(string_io)
        #
        #     #prices = pd.read_csv(url)
        #
        #
        #     if prices is not None:
        #         file_to_save = 'stock_current_price_' + date_str + '.csv'
        #         prices.to_csv(file_to_save)
        #         print('Successfully retrieved and saved ' + file_to_save)

        if prices is not None:
            all_prices.append(prices)
        else:
            print('Attempt on getting prices from the server for the date ' + date + ' has also failed.')

    if len(all_prices) > 0:
        return pd.concat(all_prices)
    else:
        print('No price files were found for the dates specified in trades.')
        return None


def load_deal_files(file_type):
    all_dfs = []
    dates = set()
    for file in glob.glob('*_' + file_type + '_*.xml'):

        with open(file) as f:
            xml2df = XML2DataFrame(f.read())
            xml_dataframe = xml2df.process_data()
            if xml_dataframe is not None:
                all_dfs.append(xml_dataframe)
            else:
                print('File %s doesn\'t contain relevant data.', file)


    if len(all_dfs) > 0:
        sem03_df = pd.concat(all_dfs)
        dates.update(sem03_df.TradeDate.astype(dt.datetime).unique())
        return sem03_df, dates

    return None, dates


def find_price(prices, board, security,trade_date, trade_time):
    relevant = prices[(prices.BOARDID == board) & (prices.SECURITYID == security) & (prices.TRADEDATE == trade_date)].copy()
    found = 'IC'
    if relevant.shape[0] == 0:
        relevant = prices[(prices.SECURITYID == security) & (prices.TRADEDATE == trade_date)].copy()
        found = ''

    if relevant.shape[0] == 0:
        return None, None, found
    else:
        relevant = relevant.sort_values('TRADETIME')
    greater_time = relevant.TRADETIME > trade_time
    if greater_time.all():
        min_time_row = relevant.iloc[0]
        if min_time_row.LastPrice > 0:
            return min_time_row.LASTPRICE, min_time_row.TRADETIME, found
        else:
            return min_time_row.LEGALCLOSE, min_time_row.TRADETIME, found
    elif not greater_time.any():
        row = relevant.iloc[-1]
        return row.CURPRICE, row.TRADETIME, found
    else:
        row = relevant[~greater_time].iloc[-1]
        return row.CURPRICE, row.TRADETIME, found




def main(path=None):
    if path:
        os.chdir(path)

    dfs = {}
    dates = set()
    for file_type in file_types:
        type_df, type_dates = load_deal_files(file_type)
        dates.update(type_dates)

        if type_df is not None:
            dfs[file_type] = type_df
            type_df.to_csv(file_type + '.csv', index=False)
        else:
            print('No %s files found or processed correctly.', file_type)

    prices = get_prices(dates, path)
    if prices is not None:
        prices.TRADETIME = prices.TRADETIME.astype(dt.datetime)
        prices.to_csv('prices.csv', index=False)
    else:
        return

    df21 = None
    if 'SEM21' in dfs.keys():
        df21 = dfs['SEM21'][ids + sem21_fields]

    df3 = None
    if 'SEM03' in dfs.keys():
        df = dfs['SEM03']
        df = df[df.TradeType.apply(lambda trade_type: trade_type not in repo_trade_types)]
        if df.shape[0]>0:
            df = process_trades_and_bids(df, prices, 'TradeTime')
            df = popuate_trades_intervals(df)
            df3 = df

    df2 = None
    if 'SEM02' in dfs.keys():
        df = dfs['SEM02']
        df = df[df.BoardName.apply(lambda name: not name.startswith('РЕПО'))]
        df = process_trades_and_bids(df, prices, 'EntryTime')
        df2 = df

    coeffs = pd.read_csv('deviationcoeffs.csv',sep=';')
    coeffs.columns = ['TradeDate', 'SecurityId'] + coeff_feilds

    if df21 is not None:
        if df3 is not None:
            df3 = df3.merge(df21, on=['BoardId', 'SecurityId'], how='left')
            df3 = df3.merge(coeffs, on=['TradeDate', 'SecurityId'], how='left')
            df3.to_csv('SEM03_prices.csv', index=False, encoding='utf-8')

        if df2 is not None:
            df2 = df2.merge(df21, on=['BoardId', 'SecurityId'], how='left')
            df2.to_csv('SEM02_prices.csv', index=False, encoding='utf-8')



def process_trades_and_bids(df, prices, time_field):
    df.loc[:, time_field] = df[time_field].astype(dt.datetime)
    df[['CurPrice', 'CurPriceTime', 'CurPriceFound']] = df.apply(
        lambda x: pd.Series(find_price(prices, x.BoardId, x.SecurityId, x.TradeDate, x[time_field])), axis=1)
    df['CurPriceRatio'] = np.abs(df.Price.astype(float) / df.CurPrice.astype(float) - 1)
    return df


def popuate_trades_intervals(df):
    df['RatioInterval'] = ''
    df.loc[(df['CurPriceRatio'] >= 0.02) & (df['CurPriceRatio'] < 0.05), 'RatioInterval'] = '{DP2}'
    df.loc[(df['CurPriceRatio'] >= 0.05) & (df['CurPriceRatio'] < 0.15), 'RatioInterval'] = '{DP5}'
    df.loc[(df['CurPriceRatio'] >= 0.15) & (df['CurPriceRatio'] < 1), 'RatioInterval'] = '{DP15}'
    return df


def popuate_bids_intervals(df):
    df['RatioInterval'] = ''
    df.loc[(df['CurPriceRatio'] >= 0.05) & (df['CurPriceRatio'] < 0.15), 'RatioInterval'] = '{ODWp5}'
    df.loc[(df['CurPriceRatio'] >= 0.15) & (df['CurPriceRatio'] < 1), 'RatioInterval'] = '{ODWp15}'
    return df


main(os.getcwd()+'/data')



