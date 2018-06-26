import math
import pandas as pd
import datetime as dt
from Import import get_prices

criterion1_types = 'TS'
criterion2_types = 'T'


def calculate_criteria(df: pd.DataFrame, sem21: pd.DataFrame, long=False):
    if long:
        return criterion5(criterion4(criterion3(criterion2(criterion1(df), sem21)), sem21), sem21)
    else:
        return criterion1(df)


def criterion1(df: pd.DataFrame):
    df1 = df[~df.ClientCode.isnull() & (df.TradeType.apply(lambda t: t == 'T'))].copy()
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
    df1 = df[~df.ClientCode.isnull() & (df.TradeType not in criterion2_types)].copy()
    #df1['temp_value'] = df1.BuySell.apply(lambda x: 1 if x == 'B' else -1) * df1.Value
    df1['pair'] = (df1[['ClientCode', 'SecurityId', 'TradeNo', 'Quantity']]
                   .groupby(['ClientCode', 'SecurityId', 'TradeNo']).transform('count') > 1)
    df1.Quantity = df1.Quantity.astype(float)
    temp = (df1[['ClientCode', 'SecurityId', 'TradeDate', 'pair', 'Quantity']].groupby(
        ['ClientCode', 'SecurityId', 'TradeDate', 'pair']).sum().groupby(level=[0, 1, 2]).apply(
        lambda x: x / x.sum()) > 0.5)
    #cr2 = temp.reset_index()
    cr2a = (temp.groupby(level=[0, 1]).sum() > 5).reset_index()
    cr2a.columns = ['ClientCode', 'SecurityId', 'CR2']
    #cr2.merge(cr2a, on=['ClientCode', 'SecurityId'], how='left')
    #cr2.CR2 = cr2.CR2 | cr2.Quantity
    df['CR2'] = df.merge(cr2a, on=['ClientCode', 'SecurityId'], how='left')['temp_value'].fillna(False)
    return df


def criterion3(df: pd.DataFrame):
    df1 = df[~df.ClientCode.isnull() & (df.TradeType =='T')].copy()
    df1['Eligible'] = df1[['ClientCode', 'SecurityId', 'TradeDate']].groupby(
        ['ClientCode', 'SecurityId']).transform('count')['TradeDate'] >= 10
    #print('Not enough information to calculate criterion 3 for ', groups[groups['TradeDate']<10].index)
    df1 = df1[df1.Eligible]

    summ_val = df1[['ClientCode', 'SecurityId', 'BuySell', 'Value']].groupby(
        ['ClientCode', 'SecurityId', 'BuySell']).sum()
    summ_qua = df1[['ClientCode', 'SecurityId', 'BuySell', 'Quantity']].groupby(
        ['ClientCode', 'SecurityId', 'BuySell']).sum()
    summ_qua.columns = ['Value']
    sum_m = summ_qua.groupby(level=[0, 1]).min()
    cost = summ_val/summ_qua
    cost_b = cost[cost.index._levels[2] == 'B'].groupby(level=[0, 1]).sum().reset_index()
    cost_s = cost[cost.index._levels[2] == 'S'].groupby(level=[0, 1]).sum().reset_index()
    cost_b.columns = ['ClientCode', 'SecurityId', 'B']
    cost_s.columns = ['ClientCode', 'SecurityId', 'S']
    sum_m.columns = ['ClientCode', 'SecurityId', 'M']

    cost = cost_b.mege(cost_s, on=['ClientCode', 'SecurityId'], how='outer').merge(sum_m, on=['ClientCode', 'SecurityId'],how='left')
    cost['fin_result'] = ((cost.B - cost.S) * cost.M).abs()
    cost['fin_result_ratio'] = (2 * cost['fin_result'] / (cost.B + cost.S) / cost.M)
    cost.loc[(cost.fin_result > 1e6) | ((cost.fin_result > 5e5) & (cost.fin_result_ratio > 0.01)), 'alarm'] = True

    temp = cost.loc[cost.alarm, ['SecurityId', 'TradeDate']].groupby('SecurityId').apply(lambda d: list(set(d.Dates.apply(lambda x: pd.date_range(start=x, periods=10, freq='B').tolist()).sum())))
    dates = temp.apply(lambda x: pd.Series(x)).stack().reset_index(level=1, drop=True).reset_index()
    prices = get_prices(path, dates)
    breaches = prices.groupby('SECURITYID').apply(get_price_diff).reset_index()
    breaches.columns = ['SecurityId', 'CR3']
    cost = cost.merge(breaches, on='SecurityId', how='left')
    cost.CR3 = cost.CR3.fillna(False) & cost.alarm
    return df.merge(cost['ClientCode', 'SecurityId', 'TradeDate',  'CR3'],
                    on=['ClientCode', 'SecurityId', 'TradeDate'], how='left')



def get_price_diff(prices: pd.DataFrame):
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
    df1 = df[(df.BoardType == 'Main') & (df.TradeType == 'T')]
    df1['Eligible'] = df1[['ClientCode', 'SecurityId', 'TradeDate']].groupby(
        ['ClientCode', 'SecurityId']).transform('count')['TradeDate'] >= 21
    df1 = df1[df1.Eligible]
    dates = df1.TradeDate.unique()
    # print('Not enough information to calculate criterion 3 for ', groups[groups['TradeDate']<10].index)

    q = df1[['SecurityId', 'Quantity']].groupby('SecurityId').sum().reset_index()
    v = sem21[sem21.TradeDate.apply(lambda date: date in dates)]['SecurityId', 'Volume'].groupby('SecurityId').sum().reset_index()
    q = q.merge(v, on='SecurityId', how='left').fillna(0.0)
    ids = q[q.Quantity/q.Volume >= 0.25]['SecurityId'].values
    df2 = df1[df1.SecurityId in ids]['ClientCode', 'SecurityId', 'Quantity'].groupby(['SecurityId', 'ClientCode']).sum()\
        .groupby(level=0).apply(lambda x: x/x.sum() > 0.02).reset_index()
    df2['CR4'] = ''
    df2.loc[df2.Quantity, 'CR4'] = 'CR4'
    return df.merge(df2['ClientCode', 'SecurityId', 'CR4'], on=['ClientCode', 'SecurityId'], how='left')


def criterion5(df: pd.DataFrame, sem21: pd.DataFrame):
    df1 = df[(df.BoardType == 'Main') & (df.TradeType == 'T')]
    df1['Eligible'] = df1[['ClientCode', 'SecurityId', 'TradeDate']].groupby(
        ['ClientCode', 'SecurityId']).transform('count')['TradeDate'] >= 21
    # print('Not enough information to calculate criterion 3 for ', groups[groups['TradeDate']<10].index)
    df1 = df1[df1.Eligible]
    dates = df1.TradeDate.unique()

    q = df1[['ClientCode', 'SecurityId', 'TradeDate', 'Quantity']].groupby(['TradeDate', 'ClientCode', 'SecurityId']).sum().reset_index()
    v = sem21[sem21.TradeDate.apply(lambda date: date in dates)]['SecurityId', 'TradeDate', 'Volume'].groupby(['TradeDate','SecurityId']).sum().reset_index()
    q = q.merge(v, on=['SecurityId', 'TradeDate'], how='left').fillna(0.0)

    q['CR5'] = q.Quantity/q.Volume >= 0.5
    q = q['ClientCode', 'TradeDate', 'CR5'].groupby(['ClientCode', 'TradeDate']).max().reset_index()
    q['CR5'] = q['CR5'] | q['ClientCode', 'TradeDate', 'CR5'].groupby(['ClientCode']).transform('sum') > 5
    return df.merge(q['ClientCode', 'TradeDate', 'CR5'], on=['ClientCode', 'TradeDate'], how='left')