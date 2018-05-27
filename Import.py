import pandas as pd
import datetime as dt
import os
import io

from Main import file_types


def import_files(path):
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

    return dfs, prices



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


