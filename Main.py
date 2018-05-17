#from input import *
import glob, os
import pandas as pd
import datetime as dt
import requests, urllib
import xml.etree.ElementTree as ET


file_types = ['SEM02', 'SEM03', 'SEM21']

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
    all_files = os.listdir(path)
    for date in dates:
        date_str = date.replace('-', '')
        date_files = list(filter(lambda file: date_str in file, all_files))
        if date_files:
            for date_file in date_files:
                with open(date_files[0]) as f:
                    xml2df = XML2DataFrame(f.read())
                    prices = xml2df.process_pricees()
                    prices['TRADEDATE'] = date
                    if prices is not None:
                        all_prices.append(prices)
                    else:
                        print('No prices data was found in ' + date_file)
        else:
            print('No price files were found for the date: ' + date + '. Trying to get from the server.')
            url = 'https://iss.moex.com/issrpc/marketdata/stock/current_price/stock_current_price_' + date_str + '.csv?tradedate=' + date
            #response = requests.get(url)
            filedata = urllib.request.urlopen(url)
            response = filedata.read().decode('utf-8')

            if response is not None:
                prices = pd.DataFrame(response)
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
    for file in glob.glob('*_' + file_type + '*.xml'):

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
        prices.to_csv('prices.csv', index=False)


main(os.getcwd()+'/data')



