import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import calendar


class Crawling:

    def __init__(self):
        self.__from_date = None
        self.__to_date = None
        self.__stock_type = None
        self.__url = r"https://www.hsx.vn/Modules/Rsde/Report/ForeignTradingReport"
        self.__mapping = {
            'Tất cả': 0,
            'Trái phiếu': 1,
            'Cổ phiếu': 2,
            'Chứng chỉ quỹ': 3,
            'ETF': 5,
            'CW': 6
        }

    @property
    def from_date(self):
        return self.__from_date

    @property
    def to_date(self):
        return self.__to_date

    @from_date.setter
    def from_date(self, value: dt.datetime):
        self.__from_date = value

    @to_date.setter
    def to_date(self, value: dt.datetime):
        self.__to_date = value

    @property
    def stock_type(self):
        return self.__stock_type

    @stock_type.setter
    def stock_type(self, value: str):
        self.__stock_type = value

    @property
    def run(self):
        from_date_str = self.from_date.strftime('%d.%m.%Y')
        to_date_str = self.to_date.strftime('%d.%m.%Y')
        stock_type = self.__mapping.get(self.stock_type)
        request_url = f"https://www.hsx.vn/Modules/Rsde/Report/ForeignTradingReport?pageFieldName1=Type&" \
                      f"pageFieldValue1={stock_type}&pageFieldOperator1=&pageFieldName2=DateFrom&pageFieldValue2={from_date_str}&" \
                      f"pageFieldOperator2=&pageFieldName3=DateTo&pageFieldValue3={to_date_str}&pageFieldOperator3=&" \
                      f"pageFieldName4=Symbol&pageFieldValue4=&pageFieldOperator4=&pageCriteriaLength=4&" \
                      f"_search=false&nd=1704370384847&rows=2147483647&page=100000&sidx=id&sord=desc"
        with requests.Session() as session:
            retry = requests.packages.urllib3.util.retry.Retry(connect=5, backoff_factor=1)
            adapter = requests.adapters.HTTPAdapter(max_retries=retry)
            session.mount('https://', adapter)
            headers = {
                'User-Agent': 'Chrome/120.0.6099.200'
            }
            html = session.get(request_url, headers=headers, timeout=10).content
            soup = BeautifulSoup(html, 'html5lib')
            news = json.loads(soup.text)
            get_cell = [d.get('cell') for d in news['rows']]
        records = []
        for cell in get_cell:
            trading_time = cell[0]
            volume_total_market = float(cell[1].replace('.', '').replace(',', '.'))
            value_total_market = float(cell[4].replace('.', '').replace(',', '.'))
            records.append((trading_time, volume_total_market, value_total_market))
        data_table = pd.DataFrame(
            data=records,
            columns=['TradingTime', 'TradingVolumeTotalMarket', 'TradingValueTotalMarket']
        )
        data_table.insert(0, 'StockType', self.stock_type)
        return data_table


if __name__ == '__main__':
    self = Crawling()
    self.stock_type = 'Cổ phiếu'
    result_list = []
    for year in range(2021, 2024):
        self.from_date = dt.datetime(year, 1, 1)
        if year == 2023:
            self.to_date = dt.datetime(year, 11, 30)
        else:
            self.to_date = dt.datetime(year, 12, 31)
        table = self.run
        result_list.append(table)

    final_table = pd.concat(result_list, ignore_index=True)
    final_table['TradingTime'] = pd.to_datetime(final_table['TradingTime']).dt.strftime('%Y-%m')
    final_table = final_table.sort_values('TradingTime', ignore_index=True)
    final_table = final_table.drop_duplicates()


