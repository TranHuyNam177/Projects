import re
import datetime as dt
import pandas as pd

import bs4
from bs4 import BeautifulSoup
import requests

from request import connect_DWH_ThiTruong
from datawarehouse import BATCHINSERT, DELETE


class MainPageCrawler:

    URL = 'https://www.hsx.vn/Modules/CMS/Web/ArticleInCategory/95cd3266-e6d1-42a3-beb5-20ed010aea4a/'
    HOME = 'https://www.hsx.vn/'

    def __init__(self):

        self.__session = None
        self.__fromDate = None
        self.__toDate = None

    def __del__(self):
        self.session.close()

    @property
    def fromDate(self):
        return self.__fromDate
    
    @fromDate.setter
    def fromDate(self,value):
        self.__fromDate = value

    @property
    def toDate(self):
        return self.__toDate

    @toDate.setter
    def toDate(self,value):
        self.__toDate = value

    @property
    def session(self):

        if self.__session is not None:
            return self.__session

        self.__session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=10)
        self.__session.mount(type(self).HOME,adapter)
        return self.__session

    @staticmethod
    def __parseData(jsonCell: list):
        
        # Thời gian
        timeString = jsonCell[1]
        processedTimeString = timeString.replace('SA','AM').replace('CH','PM')
        time = dt.datetime.strptime(processedTimeString,'%d/%m/%Y %I:%M:%S %p')
        # URL & title
        html = jsonCell[2]
        soup = BeautifulSoup(html,'html5lib')
        URL = MainPageCrawler.HOME + soup.find(name='a').get('href')
        title = soup.find(name='a').text

        return time, URL, title


    def crawl(self):

        params = {
            'exclude': '00000000-0000-0000-0000-000000000000',
            'lim': 'True',
            'pageFieldName1': 'FromDate',
            'pageFieldOperator1': 'eq',
            'pageFieldValue1': self.fromDate.strftime('%d.%m.%Y'),
            'pageFieldName2': 'ToDate',
            'pageFieldOperator2': 'eq',
            'pageFieldValue2': self.toDate.strftime('%d.%m.%Y'),
            'pageFieldName3': 'TokenCode',
            'pageFieldOperator3': 'eq',
            'pageFieldValue3': '',
            'pageFieldName4': 'CategoryId',
            'pageFieldValue4': '95cd3266-e6d1-42a3-beb5-20ed010aea4a',
            'pageFieldOperator4': 'eq',
            'pageCriteriaLength': '4',
            '_search': 'false',
            'nd': '1669361568133',
            'rows': '1000000000',
            'page': '1',
            'sidx': 'id',
            'sord': 'desc',
        }

        response = self.session.get(
            url = type(self).URL, 
            params = params,
            headers = {'User-Agent':'Chrome/100.0.4896.88'}, 
            timeout = 10,
        )

        jsonData = response.json() # dictionary
        records = []
        for row in jsonData.get('rows'):
            # Get cell value
            jsonCell = row.get('cell')
            records.append(type(self).__parseData(jsonCell))

        return pd.DataFrame(
            data = records,
            columns = ['ThoiGian', 'URL', 'TieuDe']
        ).reset_index(drop=True)


class ArticleCrawler:

    HOME = 'https://www.hsx.vn/'

    def __init__(self):

        self.__URL = None
        self.__session = None
    
    def __del__(self):
        self.session.close()

    @property
    def URL(self):
        return self.__URL

    @URL.setter
    def URL(self,value):
        self.__URL = value

    @property
    def session(self):

        if self.__session is not None:
            return self.__session

        self.__session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=10)
        self.__session.mount(type(self).HOME, adapter)
        return self.__session

    @staticmethod
    def __parseContent(tag: bs4.element.Tag):
        rawText = tag.text
        processedText = rawText.replace(u'\xa0','')
        processedText = re.sub(r'\s+',' ',processedText)
        return processedText.strip()

    def crawl(self):
        response = self.session.get(
            url = self.URL,
            headers = {'User-Agent':'Chrome/100.0.4896.88'}, 
            timeout = 10,
        )
        html = response.text
        soup = BeautifulSoup(html,'lxml')
        contentTag = soup.find(class_='unreset')
        return type(self).__parseContent(contentTag)




# Client code
def run(fromDate,toDate):

    """
    fromDate, toDate là ngày đăng ký cuối cùng
    """
    
    # Crawl bảng các article
    mainPage = MainPageCrawler()
    mainPage.fromDate = fromDate
    mainPage.toDate = toDate
    resultTable = mainPage.crawl()
    resultTable['NoiDung'] = None

    article = ArticleCrawler()
    for index in resultTable.index:
        article.URL = resultTable.loc[index,'URL']
        resultTable.loc[index,'NoiDung'] = article.crawl()

    fromDateString = fromDate.strftime('%Y-%m-%d 00:00:00')
    toDateString = toDate.strftime('%Y-%m-%d 23:59:59')

    DELETE(
        conn = connect_DWH_ThiTruong,
        table = 'TinChungQuyenHOSE',
        where = f"WHERE [ThoiGian] BETWEEN '{fromDateString}' AND '{toDateString}'"
    )

    BATCHINSERT(
        conn = connect_DWH_ThiTruong,
        table = 'TinChungQuyenHOSE',
        df = resultTable,
    )


if __name__ == '__main__':
    run(dt.datetime(2022,12,7),dt.datetime(2022,12,29))
    

