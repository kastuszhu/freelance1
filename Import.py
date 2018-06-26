import pandas as pd
import datetime as dt
import os
import glob
from XmlParsing import XML2DataFrame

#file_types = ['SEM02', 'SEM03', 'SEM21']
file_types = ['SEM03', 'SEM21']
import_path = None
import_dates = [dt.date.today()]


def import_files():
    if import_path:
        os.chdir(import_path)

    dfs = {}
    for file_type in file_types:
        type_df  = load_deal_files(file_type)

        if type_df is not None:
            dfs[file_type] = type_df
            #type_df.to_csv(file_type + '.csv', index=False)
        else:
            print('No %s files found or processed correctly.', file_type)

    prices = get_prices()
    if prices is not None:
        prices.TRADETIME = pd.to_datetime(prices.TRADETIME)
        #prices.to_csv('prices.csv', index=False)

    return dfs, prices


def get_prices(dates=None):
    all_prices = []
    price_files = list(filter(lambda x: 'stock_current_price_' in x, os.listdir()))

    sec_dict = None
    #dates_list = []
    if dates is None:
        dates_list = import_dates
    elif type(dates) == pd.DataFrame:
        sec_dict = dates.groupby(0).apply(lambda x: x['SecurityId'].values).to_dict()
        dates_list = list(sec_dict.keys())
    else:
        dates_list = dates

    for date in dates_list:
        date_str = ''
        if type(date) is str:
            date_str = date.replace('-', '')
        else:
            date_str = date.strftime("%Y%m%d")

        date_files = list(filter(lambda file: date_str in file, price_files))
        prices = None
        if len(date_files) > 0:
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
            #print('No price files were found for the date: ' + date + '. Trying to get from the server.')
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
            if (sec_dict is not None) and (date in sec_dict.keys()):
                prices = prices[prices.TRADEDATE.isin(sec_dict[date])]
            all_prices.append(prices)
        else:
            #print('Attempt on getting prices from the server for the date ' + date + ' has also failed.')
            print('No price files were found for the date: ' + date.strftime("%Y%m%d") + '.')

    if len(all_prices) > 0:
        return pd.concat(all_prices)
    else:
        print('No price files were found for the specified dates.')
        return None


def load_deal_files(file_type):
    all_dfs = []
    dates = [date.strftime("%d%m%y") for date in import_dates]
    xml_files = list(filter(lambda file: any([date in file for date in dates]), glob.glob('*_' + file_type + '_*.xml')))
    csv_files = list(filter(lambda file: any([date in file for date in dates]), glob.glob('*_' + file_type + '_*.csv')))

    for file in xml_files:
        with open(file) as f:
            xml2df = XML2DataFrame(f.read())
            xml_dataframe = xml2df.process_data()
            if xml_dataframe is not None:
                all_dfs.append(xml_dataframe)
            else:
                print('File %s doesn\'t contain relevant data.', file)

    for file in csv_files:
        df = pd.read_csv(file, sep=';')
        all_dfs.append(df)

    if len(all_dfs) > 0:
        sem03_df = pd.concat(all_dfs)
        #missing_dates = set(import_dates) - set(pd.to_datetime(sem03_df.TradeDate).unique())
        #print('SEM03 files were not loaded for the next dates: ' + ('%-2s ' * len(missing_dates))[:-1] % tuple(missing_dates))
        if len(all_dfs) < 20 and len(dates) > 1:
            print('Number of loaded ' + file_type + ' files is less than 20')
        return sem03_df

    return None


