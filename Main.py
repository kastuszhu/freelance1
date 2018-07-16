import os
import pandas as pd
import numpy as np
import datetime as dt
import Import
import Criteria

repo_trade_types = 'RHJLM'



def find_price(prices, board, security, trade_date, trade_time):
    relevant = prices[
        (prices.BOARDID == board) & (prices.SECURITYID == security) & (prices.TRADEDATE == trade_date)].copy()
    found = '+'
    if relevant.shape[0] == 0:
        relevant = prices[(prices.SECURITYID == security) & (prices.TRADEDATE == trade_date)].copy()
        found = 'IC'

    if relevant.shape[0] == 0:
        return None, None, found, None, None, None, None
    else:
        relevant = relevant.sort_values('TRADETIME')
    greater_time = relevant.TRADETIME > trade_time
    if greater_time.all():
        min_time_row = relevant.iloc[0]
        if min_time_row.LASTPRICE > 0:
            return min_time_row.LASTPRICE, min_time_row.TRADETIME, found, None, None, None, None
        else:
            return min_time_row.LEGALCLOSE, min_time_row.TRADETIME, found, None, None, None, None
    elif not greater_time.any():
        row = relevant.iloc[-1]
        if found == '+':
            return row.CURPRICE, row.TRADETIME, found, None, None, None, None
        else:
            return row.CURPRICE, row.TRADETIME, found, row.CURPRICE, row.LASTPRICE, row.LEGALCLOSE, row.TRADETIME
    else:
        row = relevant[~greater_time].iloc[-1]
        return row.CURPRICE, row.TRADETIME, found, None, None, None, None


def process_trades_and_bids(df: pd.DataFrame, prices: pd.DataFrame, time_field: str, filterr):
    df.loc[:, time_field] = pd.to_datetime(df[time_field])
    df[['CurPrice', 'CurPriceTime', 'CurPriceFound', 'LastP1', 'LastP2', 'LastP3', 'LastTime']] = df.apply(
        lambda x: pd.Series(find_price(prices, x.BoardId, x.SecurityId, x.TradeDate, x[time_field])), axis=1)
    df.loc[filterr, 'CurPriceRatio'] = (df[filterr].Price.astype(float) / df[filterr].CurPrice.astype(float) - 1).abs()
    return df


def populate_trades_intervals(df):
    df['RatioInterval'] = ''
    df.loc[(df['CurPriceRatio'] >= 0.02) & (df['CurPriceRatio'] < 0.05), 'RatioInterval'] = '{DP2}'
    df.loc[(df['CurPriceRatio'] >= 0.05) & (df['CurPriceRatio'] < 0.15), 'RatioInterval'] = '{DP5}'
    df.loc[(df['CurPriceRatio'] >= 0.15) & (df['CurPriceRatio'] < 1), 'RatioInterval'] = '{DP15}'
    return df


def populate_bids_intervals(df):
    df['RatioInterval'] = ''
    df.loc[(df['CurPriceRatio'] >= 0.05) & (df['CurPriceRatio'] < 0.15), 'RatioInterval'] = '{ODWp5}'
    df.loc[(df['CurPriceRatio'] >= 0.15) & (df['CurPriceRatio'] < 1), 'RatioInterval'] = '{ODWp15}'
    return df


def check_volume_breaches(df, df21):
    df = df.merge(df21, on=['BoardId', 'SecurityId', 'TradeDate'], how='left')
    df.Volume = df.Volume.astype(float)
    df.MP3ValTrd = df.MP3ValTrd.astype(float)
    df['VolumeRate'] = ''
    df.loc[df.MP3ValTrd > 0, 'VolumeRate'] = df.loc[df.MP3ValTrd > 0, 'Volume'] / \
                                             df.loc[df.MP3ValTrd > 0, 'MP3ValTrd']
    df['VolumeRateBreach'] = ''
    df.loc[df.MP3ValTrd > 0, 'VolumeRateBreach'] = \
        df.loc[df.MP3ValTrd > 0, 'VolumeRate'].apply(lambda r: 'Over 5%' if r > 0.05 else '')
    return df


z = pd.Timedelta('0sec')
h6 = pd.Timedelta('6h')
h3 = pd.Timedelta('3h')
h2 = pd.Timedelta('2h')
h1 = pd.Timedelta('1h')

def if_filtering(x, if_messages: pd.DataFrame, blue_chips):
    r = (if_messages.SecurityId == x.SecurityId)
    if x.SecurityId in blue_chips:
         r1 =  ((x.SecurityType != 'об') & ((if_messages.message_time - x.TradeTime < h3) & (if_messages.message_time - x.TradeTime > z))) |\
         ((x.SecurityType == 'об') & ((if_messages.message_time - x.TradeTime < h6) & (if_messages.message_time - x.TradeTime > z)))
    else:
        r1 = ((x.SecurityType != 'об') & (
        (if_messages.message_time - x.TradeTime < h1) & (if_messages.message_time - x.TradeTime > z))) | \
             ((x.SecurityType == 'об') & (
             (if_messages.message_time - x.TradeTime < h3) & (if_messages.message_time - x.TradeTime > z)))

    return r & r1


def check_if(df, if_messages, blue_chips):
    df.TradeTime = pd.to_datetime(df.apply(lambda x: str(x.TradeDate) + ' ' + str(x.TradeTime), axis=1))
    df['interfax'] = df.apply(lambda x: '; '.join(if_messages[if_filtering(x, if_messages, blue_chips)].message_id),axis=1)
    return df


def main(path=None):
    Import.import_path = path

    # if date is None:
    #     date = dt.date.today()
    #
    # if long:
    #     Import.import_dates = pd.date_range(end=date, periods=30)
    # else:
    #Import.import_dates = [date]

    dfs, prices = Import.import_files()

    if (len(dfs) == 0) or (prices is None):
        return

    df3 = None
    if 'SEM03' in dfs.keys():
        df = dfs['SEM03']
        if df.shape[0] > 0:
            filterr = df.TradeType.apply(lambda trade_type: trade_type not in repo_trade_types)
            df = process_trades_and_bids(df, prices, 'TradeTime', filterr)
            df = populate_trades_intervals(df)
            df3 = df

    df2 = None
    if 'SEM02' in dfs.keys():
        df = dfs['SEM02']
        df = Criteria.process_fr(df, Criteria.limit_type_codes, ['R', 'FR'])
        df = Criteria.process_fr(df, Criteria.market_type_codes, ['P', 'PR'])
        # filterr = df.BoardName.apply(lambda name: not name.startswith('РЕПО'))
        # df = process_trades_and_bids(df, prices, 'EntryTime')
        df2 = df



    if 'SEM21' in dfs.keys():
        df21 = dfs['SEM21']
        df21['Volume'] = df21['Volume'].astype(float)
        if df3 is not None:
            df3 = check_volume_breaches(df3, df21)

            try:
                coeffs = Import.import_coeffs()
                if coeffs is not None:
                    df3 = df3.merge(coeffs, on=['TradeDate', 'SecurityId'], how='left')
            except:
                print('Couldn\'t import coeffs')

            df3 = Criteria.calculate_criteria(df3, df21, True)

            try:
                if_messages = Import.import_if(pd.to_datetime(df3.TradeDate.unique()))
                df3 = check_if(df3, if_messages, [])
            except Exception as e:
                print(f'Couldn\'t check interfax: {e}')

            df3.to_excel('SEM03_result.xlsx', index=False, encoding='utf-8')


        if df2 is not None:
            df2 = df2.merge(df21, on=['BoardId', 'SecurityId', 'TradeDate'], how='left')
            df2.to_excel('SEM02_result.xlsx', index=False, encoding='utf-8')
            if df3 is not None:
                df3 = df3.merge(df2[['OrderNo', 'R', 'FR', 'P', 'PR']], on='OrderNo', how='left')
                df3.to_excel('SEM03_result.xlsx', index=False, encoding='utf-8')




main(os.getcwd() + '/data')
