import math
from typing import List

import pandas as pd
import datetime as dt
from Import import get_prices

criterion1_types = 'TS'
criterion2_types = ['T']
limit_type_codes = [
    'NLP',
    'NLW',
    'NM',
    'NPM',
]
market_type_codes = [
    'NSQ',
    'NSC',
]

def calculate_criteria(df: pd.DataFrame, sem21: pd.DataFrame, long=False):
    if long:
        return criterion5(
            criterion4(
                criterion3(
                    criterion2(
                        criterion1(
                            df
                        ),
                        sem21
                    )),
                sem21),
            sem21)
        # return criterion5(criterion4(criterion1(df), sem21), sem21)
    else:
        return criterion1(df)


def criterion1(df: pd.DataFrame):
    df1 = df[~df.ClientCode.isnull() & (df.TradeType.apply(lambda t: t == 'T'))].copy()
    if df1.shape[0] == 0:
        return df
    df1.Value = df1.Value.astype(float)
    df1['temp_value'] = df1.apply(lambda x: x.Value if (x.BuySell == 'B') else -x.Value, axis=1).astype(float)
    date_sum = df1[['ClientCode', 'SecurityId', 'TradeDate', 'temp_value']].groupby(
        ['ClientCode', 'SecurityId', 'TradeDate']).sum()
    cr1 = (date_sum > 8e+7).reset_index()
    cr1_long = ((date_sum > 8e+7).astype(int).groupby(level=[0, 1]).sum() > 5).reset_index()
    cr1_long_sum = (df1[['ClientCode', 'SecurityId', 'temp_value']].groupby(
        ['ClientCode', 'SecurityId']).sum() > 2e+9).reset_index()
    cr1_long.columns = list(cr1_long.columns[:-1]) + ['CR1']
    cr1_long_sum.columns = list(cr1_long_sum.columns[:-1]) + ['CR1_sum']
    cr1 = cr1.merge(cr1_long, on=['ClientCode', 'SecurityId'], how='left')\
             .merge(cr1_long_sum, on=['ClientCode', 'SecurityId'], how='left')
    cr1.CR1 = cr1.CR1 | cr1.temp_value | cr1.CR1_sum
    cr1.drop(['temp_value', 'CR1_sum'], axis=1, inplace=True)
    df['CR1'] = df.merge(cr1, on=['ClientCode', 'SecurityId', 'TradeDate'], how='left')['CR1'].fillna(False)
    return df


def criterion2(df: pd.DataFrame, sem21: pd.DataFrame):
    df1 = df[~df.ClientCode.isnull() & df.TradeType.isin(criterion2_types)].copy()
    df1['pair'] = (df1[['SecurityId', 'TradeNo']]
                   .groupby(['TradeNo']).transform('count') > 1)
    df1.Quantity = df1.Quantity.astype(float)
    pair_sum = df1[['TradeDate', 'ClientCode', 'SecurityId', 'pair', 'Quantity']].groupby(
        ['TradeDate', 'ClientCode', 'SecurityId', 'pair']).sum()

    pair_sum.to_excel('cr2_sum.xlsx')
    share = pair_sum.groupby(level=[0, 1, 2]).transform(lambda x: x / x.sum())
    share2 = pair_sum.groupby(level=[0, 1]).transform(lambda x: x / x.sum())
    pair_sum['share'] = share
    pair_sum['share2'] = share2
    pair_sum = pair_sum.reset_index()
    pair_sum = pair_sum[pair_sum.pair]
    pair_sum = pair_sum.merge(sem21[['SecurityId', 'TradeDate', 'Volume']], on=['SecurityId', 'TradeDate'], how='left')
    pair_sum['CR2'] = False
    pair_sum.loc[((pair_sum.share>0.5) | (pair_sum.share2>0.5)) & (pair_sum.Quantity/pair_sum.Volume > 0.05), 'CR2'] = True

    cr2a = (pair_sum[['ClientCode', 'SecurityId', 'CR2']].groupby(['ClientCode', 'SecurityId']).sum() > 5).reset_index()
    cr2a.columns = ['ClientCode', 'SecurityId', 'CR2a']
    pair_sum = pair_sum.merge(cr2a, on=['ClientCode', 'SecurityId'], how='left')
    pair_sum.to_excel('cr2.xlsx', index=False)
    pair_sum.CR2 = pair_sum.CR2 | pair_sum.CR2a
    df['CR2'] = df.merge(pair_sum, on=['ClientCode', 'SecurityId'], how='left')['CR2'].fillna(False)
    return df


def criterion3(df: pd.DataFrame):
    df1 = df[~df.ClientCode.isnull() & (df.TradeType =='T')].copy()
    if df.TradeDate.unique().shape[0] < 10:
        print ('Not trade data to calculate criterion 3.')
        df['CR3'] = False
        return df
    else:
        summ_val = df1[['ClientCode', 'SecurityId', 'BuySell', 'Value']].groupby(
            ['ClientCode', 'SecurityId', 'BuySell']).sum()
        summ_qua = df1[['ClientCode', 'SecurityId', 'BuySell', 'Quantity']].groupby(
            ['ClientCode', 'SecurityId', 'BuySell']).sum()
        summ_qua.columns = ['Value']
        sum_m = summ_qua.groupby(level=[0, 1]).min().reset_index()
        cost = (summ_val/summ_qua).reset_index()
        cost_b = cost[cost.BuySell == 'B'].groupby(['ClientCode', 'SecurityId']).sum().reset_index()
        cost_s = cost[cost.BuySell == 'S'].groupby(['ClientCode', 'SecurityId']).sum().reset_index()
        cost_b.columns = ['ClientCode', 'SecurityId', 'B']
        cost_s.columns = ['ClientCode', 'SecurityId', 'S']
        sum_m.columns = ['ClientCode', 'SecurityId', 'M']

        cost = cost_b.merge(cost_s, on=['ClientCode', 'SecurityId'], how='outer').merge(sum_m, on=['ClientCode', 'SecurityId'],how='left')
        cost['fin_result'] = ((cost.B - cost.S) * cost.M).abs()
        cost['fin_result_ratio'] = (2 * cost['fin_result'] / (cost.B + cost.S) / cost.M)
        cost['alarm'] = False
        cost.loc[(cost.fin_result > 1e6) | ((cost.fin_result > 5e5) & (cost.fin_result_ratio > 0.01)), 'alarm'] = True

        if cost.alarm.any():
            temp = cost.loc[cost.alarm, ['SecurityId', 'TradeDate']].groupby('SecurityId').apply(
                lambda d: list(set(d.Dates.apply(lambda x: pd.date_range(start=x, periods=10, freq='B').tolist()).sum())))
            dates = temp.apply(lambda x: pd.Series(x)).stack().reset_index(level=1, drop=True).reset_index()
            prices = get_prices(dates)
            breaches = prices.groupby('SECURITYID').apply(get_price_diff).reset_index()
            breaches.columns = ['SecurityId', 'CR3']
            cost = cost.merge(breaches, on='SecurityId', how='left')
        else:
            cost['CR3'] = False
        cost.to_excel('cr3.xlsx', index=False)
        cost.CR3 = cost.alarm
        return df.merge(cost[['ClientCode', 'SecurityId', 'CR3']],
                        on=['ClientCode', 'SecurityId'], how='left')



def get_price_diff(prices: pd.DataFrame):
    prices = prices.sort_values(['TRADEDATE', 'TRADETIME'])
    prices['PRICEDIFF'] = prices.CURPRICE.shift(1)
    prices['PRICEDIFF'] = (prices.CURPRICE - prices.PRICEDIFF).abs()/prices.PRICEDIFF

    prices['TIMEDIFF'] = prices.TRADETIME.shift(1)
    prices.TIMEDIFF = prices.TRADETIME - prices.TIMEDIFF

    prices['BREACH'] = False
    prices.loc[(prices.TIMEDIFF <= dt.timedelta(minutes=2)) & (prices.PRICEDIFF > 0.002), 'BREACH'] = True
    prices.loc[(dt.timedelta(minutes=2) < prices.TIMEDIFF) & (prices.TIMEDIFF <= dt.timedelta(minutes=5)) & (prices.PRICEDIFF > 0.0033), 'BREACH'] = True
    prices.loc[(dt.timedelta(minutes=5) < prices.TIMEDIFF) & (prices.PRICEDIFF > 0.0045), 'BREACH'] = True
    prices.drop(['PRICEDIFF', 'TIMEDIFF'], inplace=True)
    return prices.BREACH.any()


def criterion4(df: pd.DataFrame, sem21: pd.DataFrame):
    #df1 = df[(df.BoardType == 'Main') & (df.TradeType == 'T')]
    df1 = df[(df.TradeType == 'T')].copy()
    #df1['Eligible'] = df1[['ClientCode', 'SecurityId', 'TradeDate']].groupby(
    #    ['ClientCode', 'SecurityId']).transform('count')['TradeDate'] >= 20
    #df1 = df1[df1.Eligible]
    dates = df1.TradeDate.unique()
    # print('Not enough information to calculate criterion 3 for ', groups[groups['TradeDate']<10].index)

    q = df1[['SecurityId', 'Quantity']].groupby('SecurityId').sum().reset_index()
    v = sem21[sem21.TradeDate.isin(dates)][['SecurityId', 'Volume']].groupby('SecurityId').sum().reset_index()
    #v = sem21[['SecurityId', 'Volume']].groupby('SecurityId').sum().reset_index()
    q = q.merge(v, on='SecurityId', how='left').fillna(0.0)
    ids = q[q.Quantity/q.Volume >= 0.25]['SecurityId'].values
    df2 = df1[df1.SecurityId.isin(ids)][['ClientCode', 'SecurityId', 'Quantity']].groupby(['SecurityId', 'ClientCode']).sum()\
        .groupby(level=0).apply(lambda x: x/x.sum() > 0.02).reset_index()
    df2['CR4'] = False
    df2.loc[df2.Quantity, 'CR4'] = True
    df = df.merge(df2[['ClientCode', 'SecurityId', 'CR4']], on=['ClientCode', 'SecurityId'], how='left')
    df.CR4 = df.CR4.fillna(False)
    return df


def criterion5(df: pd.DataFrame, sem21: pd.DataFrame):
    #df1 = df[(df.BoardType == 'Main') & (df.TradeType == 'T')]
    df1 = df[(df.TradeType == 'T')].copy()
    #df1['Eligible'] = df1[['ClientCode', 'SecurityId', 'TradeDate']].groupby(
    #    ['ClientCode', 'SecurityId']).transform('count')['TradeDate'] >= 20
    # print('Not enough information to calculate criterion 3 for ', groups[groups['TradeDate']<10].index)
    #df1 = df1[df1.Eligible]
    dates = df1.TradeDate.unique()

    q = df1[['ClientCode', 'SecurityId', 'TradeDate', 'Quantity']].groupby(['TradeDate', 'ClientCode', 'SecurityId']).sum().reset_index()
    v = sem21[sem21.TradeDate.isin(dates)][['SecurityId', 'TradeDate', 'Volume']].groupby(['TradeDate','SecurityId']).sum().reset_index()
    #v = sem21[['SecurityId', 'TradeDate', 'Volume']].groupby(['TradeDate','SecurityId']).sum().reset_index()
    q = q.merge(v, on=['SecurityId', 'TradeDate'], how='left').fillna(0.0)
    q.to_excel('cr5.xlsx',index=False)

    q['CR5'] = q.Quantity/q.Volume >= 0.5
    #q = q[['ClientCode', 'TradeDate', 'CR5']].groupby(['ClientCode', 'TradeDate']).max().reset_index()
    long_term = (q[['ClientCode', 'SecurityId', 'CR5']].groupby(['ClientCode', 'SecurityId']).transform('sum') > 5)
    q['CR5'] = q['CR5'] | long_term.CR5
    df = df.merge(q[['ClientCode','SecurityId', 'TradeDate', 'CR5']], on=['ClientCode', 'SecurityId', 'TradeDate'], how='left')
    df.CR5 = df.CR5.fillna(False)
    return df


td = pd.Timedelta('30sec')
tdz = pd.Timedelta('0sec')


def fr_filtering(g, x):
    r = (((g.EntryTime - x.EntryTime) >= -td) & (tdz > (g.EntryTime - x.EntryTime))) #& (g.ClientCode != x.ClientCode)
    if r.any():
        r[r] = g[r][['TrdAccId', 'EntryTime']].groupby('TrdAccId').transform(lambda _: _.count() > 1).EntryTime |  \
               g[r]['TrdAccId'].apply(lambda _: _.startswith('L'))
    return r


def process_fr(df, type_codes, headers):
    df = df.sort_values('EntryTime')
    df1 = df[df.OrdTypeCode.isin(type_codes)].copy()
    if df1.shape[0] == 0:
        df[headers[0]] = ''
        df[headers[1]] = ''
        return df
    df1.EntryTime = pd.to_datetime(df1.EntryTime)
    gr_all = []

    for key, gr in df1.groupby(['TradeDate', 'SecurityId']):
        group = gr.copy()
        group[headers[0]] = \
            group.apply(lambda x:
                        '; '.join(
                            group[fr_filtering(group,x)]
                                .OrderNo.astype(str).values[1:]), axis=1)

        group[headers[1]] = \
            group.apply(lambda x:
                        '; '.join(
                            group[(((group.EntryTime - x.EntryTime) <= td) & (tdz < (group.EntryTime - x.EntryTime)))]
                                .OrderNo.astype(str).values[1:]), axis=1)
        gr_all.append(group)

    if len(gr_all) > 0:
        df = df.merge(pd.concat(gr_all)[['OrderNo'] + headers], on='OrderNo')
    return df

