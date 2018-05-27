import glob, os
import pandas as pd
import numpy as np
import datetime as dt
import Import


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

    dfs, prices = Import.import_files(path)

    if (len(dfs)==0) or (prices is None):
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



