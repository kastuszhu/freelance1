import os
import pandas as pd
import numpy as np
import datetime as dt
import Import
import Criteria

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
coeff_fields = ['liquidity', 'sigma', 'beta', 'f_plus', 'f_minus', 'spread', 'coeff_c']


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


def process_trades_and_bids(df: pd.DataFrame, prices: pd.DataFrame, time_field: str):
    df.loc[:, time_field] = pd.to_datetime(df[time_field])
    df[['CurPrice', 'CurPriceTime', 'CurPriceFound', 'LastP1', 'LastP2', 'LastP3', 'LastTime']] = df.apply(
        lambda x: pd.Series(find_price(prices, x.BoardId, x.SecurityId, x.TradeDate, x[time_field])), axis=1)
    df['CurPriceRatio'] = (df.Price.astype(float) / df.CurPrice.astype(float) - 1).abs()
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


def main(path=None, date=None, long=False):
    Import.import_path = path

    if date is None:
        date = dt.date.today()

    if long:
        Import.import_dates = pd.date_range(end=date, periods=30)
    else:
        Import.import_dates = [date]

    dfs, prices = Import.import_files()

    if (len(dfs) == 0) or (prices is None):
        return

    df21 = None
    if 'SEM21' in dfs.keys():
        df21 = dfs['SEM21'][ids + sem21_fields]

    df3 = None
    if 'SEM03' in dfs.keys():
        df = dfs['SEM03']
        df = df.loc[df.TradeType.apply(lambda trade_type: trade_type not in repo_trade_types)].copy()
        if df.shape[0] > 0:
            df = process_trades_and_bids(df, prices, 'TradeTime')
            df = populate_trades_intervals(df)
            df3 = df

    df2 = None
    if 'SEM02' in dfs.keys():
        df = dfs['SEM02']
        df = df[df.BoardName.apply(lambda name: not name.startswith('РЕПО'))].copy()
        df = process_trades_and_bids(df, prices, 'EntryTime')
        df2 = df

    coeffs = pd.read_csv('deviationcoeffs.csv', sep=';')
    coeffs.columns = ['TradeDate', 'SecurityId'] + coeff_fields

    if df21 is not None:
        if df3 is not None:
            df3 = df3.merge(df21, on=['BoardId', 'SecurityId'], how='left')
            df3.Volume = df3.Volume.astype(float)
            df3.MP3ValTrd = df3.MP3ValTrd.astype(float)
            df3['VolumeRate'] = ''
            df3.loc[df3.MP3ValTrd > 0, 'VolumeRate'] = df3.loc[df3.MP3ValTrd > 0, 'Volume'] / \
                                                       df3.loc[df3.MP3ValTrd > 0, 'MP3ValTrd']
            df3['VolumeRateBreach'] = ''
            df3.loc[df3.MP3ValTrd > 0, 'VolumeRateBreach'] = \
                df3.loc[df3.MP3ValTrd > 0, 'VolumeRate'].apply(lambda r: 'Over 5%' if r > 0.05 else '')

            df3 = df3.merge(coeffs, on=['TradeDate', 'SecurityId'], how='left')
            df3 = Criteria.calculate_criteria(df3, df21)
            df3.to_excel('SEM03_result.xlsx', index=False, encoding='utf-8')


        if df2 is not None:
            df2 = df2.merge(df21, on=['BoardId', 'SecurityId'], how='left')
            df2.to_excel('SEM02_result.xlsx', index=False, encoding='utf-8')


main(os.getcwd() + '/data', dt.date(2017, 6, 7), False)
