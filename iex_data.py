import requests
import pandas as pd
import numpy as np
import datetime

class IEX():
    def __init__(self, securities=None, begin=None, end=datetime.datetime.now().date(), endpoint='https://api.iextrading.com/1.0/stock/market/batch'):
        self.securities = securities
        self.begin = begin
        self.end = end
        if begin != None:
            self.time_period = self.end - self.begin
        self.endpoint = endpoint

    @staticmethod
    def df_compiler():
        stocks = IEX()
        stocks.symbols_get('cs')
        company_df = stocks.company_info_get(['industry', 'sector'])
        company_df = pd.get_dummies(company_df, ['industry', 'sector'])
        shares_df = stocks.company_info_get(['sharesOutstanding'],'stats')
        # earnings_df = stocks.earnings_info_get(['actualEPS'])
        financials_df = stocks.financials_info_get('annual')
        prices_df = stocks.price_get('1y')
        average_price = prices_df.mean(axis=0).rename('avgPrice')
        df = company_df.join(financials_df).join(shares_df).join(average_price)
        df['marketCap'] = df['sharesOutstanding'] * df['avgPrice']
        df.drop(['sharesOutstanding', 'avgPrice'], axis=1, inplace=True)
        return df

    # single get request to API
    def _single_query(self, payload):
        response = requests.get(self.endpoint, params=payload)
        if response.status_code != 200:
            print('request unsuccessful')
        else:
            response_j = response.json()
        return response_j

    # function to step through list of symbols 'size' at a time
    def _chunker(self, seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    # function to replace None with 0
    def _replace_none(self, x):
        for i, val in enumerate(x):
            if val == None:
                x[i] = 0
        return x

    # query API for comprensive list of security symbols available
    def symbols_get(self, type=None, endpoint='https://api.iextrading.com/1.0/ref-data/symbols'):
        symbols = requests.get(endpoint).json()
        # Filter symbols to create list of common stocks only
        symbols_list = []
        if type == None:
            for sym in symbols:
                symbols_list.append(sym['symbol'])
        else:
            for sym in symbols:
                if sym['type'] == type:
                    symbols_list.append(sym['symbol'])
        self.securities = symbols_list

    # query API for company info and return df with tickers as index and columns as company info
    def company_info_get(self, parameters, cat='company'):
        symbol_dict = {}
        param_string = (', ').join(parameters)
        # securities = self.securities
        for group in self._chunker(self.securities, 100):
            payload = {'filter': param_string, 'types': cat, 'symbols':(', ').join(group)}
            response = self._single_query(payload)
            group_dict = {}
            for ticker in group:
                group_dict[ticker] =[response[ticker][cat][param]\
                for param in parameters]
            symbol_dict.update(group_dict)
        company_info_df = pd.DataFrame(columns=parameters, index=symbol_dict.keys(), data=list(symbol_dict.values()))
        return company_info_df


    def earnings_info_get(self, parameters, cat='earnings'):
        param_string = (', ').join(parameters)
        symbol_dict = {}
        for group in self._chunker(self.securities, 100):
            group_dict = {}
            payload = {'filter': param_string, 'types': cat, 'symbols':(', ').join(group)}
            response = self._single_query(payload)
            for ticker in group:
                try:
                    group_dict[ticker] =[dict[param]\
                    for param in parameters \
                    for dict in response[ticker][cat][cat]]
                except:
                    group_dict[ticker] = [np.nan]
            symbol_dict.update(group_dict)
        earnings_df = pd.DataFrame(columns=parameters*4, index=symbol_dict.keys(), data=list(symbol_dict.values()))
        return earnings_df


    def financials_info_get(self, period, cat='financials'):
        for ticker in self.securities:
            if ticker not in self.symbol_get('cs'):
                return ".securities attribute must be set to common stocks only"
        symbol_dict = {}
        for group in self._chunker(self.securities, 100):
            group_dict = {}
            payload = {'types': cat, 'symbols':(', ').join(group), 'period': period}
            response = self._single_query(payload)
            for ticker in group:
                try:
                    group_dict[ticker] = list(response[ticker][cat][cat][0].values())
                except:
                    group_dict[ticker] = [np.nan]
            symbol_dict.update(group_dict)
        financials_keys = list(response[ticker]['financials']['financials'][0].keys())
        financials_df = pd.DataFrame(index=symbol_dict.keys(), columns=financials_keys, data=list(symbol_dict.values()) )
        return financials_df

    def price_get(self, range, symbols=None, cat='chart'):
        if symbols:
            self.securities = symbols
        symbol_dict = {}
        for group in self._chunker(self.securities, 100):
            group_dict = {}
            payload = {'filter': 'close, date', 'types': cat, 'symbols': (', ').join(group), 'range': range}
            response = self._single_query(payload)
            for ticker in group:
                try:
                    closing_prices = {}
                    for dict in response[ticker][cat]:
                        closing_prices[dict['date']] = [dict['close']]
                    if closing_prices == {}:
                        group_dict[ticker] = [np.nan]
                    else:
                        group_dict[ticker] = closing_prices
                except:
                    group_dict[ticker] = [np.nan]
            symbol_dict.update(group_dict)
        price_df = pd.DataFrame()
        for k, v in symbol_dict.items():
            if v == [np.nan]:
                price_df[k] = np.nan
            else:
                join_df = pd.DataFrame(v).T
                join_df.columns = [k]
                price_df = price_df.join(join_df, how='outer')
        price_df.index = pd.to_datetime(price_df.index)
        return price_df
