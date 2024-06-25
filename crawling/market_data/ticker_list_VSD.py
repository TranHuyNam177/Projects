import re
import datetime as dt
import time

import requests
import pandas as pd

from os.path import dirname, join
from pandas import DataFrame
from bs4 import BeautifulSoup
from itertools import chain
from unidecode import unidecode

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options

from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import ElementClickInterceptedException

from request import connect_DWH_ThiTruong
from datawarehouse import BATCHINSERT, DELETE

PATH = join(dirname(dirname(dirname(__file__))), 'dependency', 'chromedriver')


class CrawlException(Exception):
    pass


class Crawler:

    def __init__(
            self
    ):
        self.__driver = None
        self.__wait = None
        self.__tableColNum = 0

        self.__ignored_exceptions = (
            ValueError,
            IndexError,
            NoSuchElementException,
            StaleElementReferenceException,
            TimeoutException,
            ElementNotInteractableException,
            ElementClickInterceptedException,
        )

    @property
    def driver(
            self
    ):
        option = Options()
        # Initialize the driver for Google Chrome
        driver = webdriver.Chrome(
            executable_path=(PATH),
            options=option
        )
        # Open the initial window web page
        driver.get(f"https://vsd.vn/vi/is?tab=5&page=1")
        # Create the implicit wait for the driver with timeout = 30 seconds
        self.__wait = WebDriverWait(
            driver=driver,
            timeout=30,
            ignored_exceptions=self.__ignored_exceptions
        )
        return driver

    def __getPageDataFromPageTableSoup(
            self,
            tableSoup: BeautifulSoup,
            pageIndex: int
    ) -> list:
        tableElements = tableSoup.findAll('td')
        # Retry to get data if the page misses information
        timeoutIndex = 0
        while not tableSoup.findAll('td'):
            timeoutIndex += 1
            # Raise timeout error if there are 10 consecutive failed crawls
            if timeoutIndex > 20:
                raise CrawlException("Không có nội dung trong HTML!")
            tableSoup = self.__getTableSoupFromOnePage(pageIndex)
            tableElements = tableSoup.findAll('td')

        # Get the text of table contents in each page
        tableContents = (cell.text for cell in tableElements if not re.search(r'^\d+$', cell.text))
        # Get all URLs from the table in a page
        pageURLs = self.__getURLsFromOnePage(tableSoup)

        # Get the text of all cells each row in the table
        tableRowIterator = (tableContents,) * self.__tableColNum
        # Convert the list of all cells in the table into the list of 8-elements tuples
        # ('URL', 'MaCK', 'MaISIN', 'TenCK', 'LoaiCK', 'SanGiaoDich', 'NoiQuanLy', 'TrangThai')
        tableRowTuple = zip(*tableRowIterator)
        # Loop through each tuple in the list with index number
        # Combine the tuple of time, tuple of cells, and tuple of URLs together to generate a complete 9 elements tuple
        try:
            pageData = [(pageURLs[rowIndex],) + rowText for rowIndex, rowText in enumerate(tableRowTuple)]
        except(Exception, ):
            pageData = []
        print(pageData)
        return pageData

    @staticmethod
    def __getURLsFromOnePage(
            tableSoup: BeautifulSoup
    ) -> tuple:
        # Get the urls of the table
        # Remove duplicate urls on each row in the table by using dict.fromkeys()
        pageURLs = dict.fromkeys('https://vsd.vn' + url['href'] for url in tableSoup.findAll('a'))
        return tuple(pageURLs)

    def __getTableSoupFromOnePage(
            self,
            pageIndex: int
    ) -> BeautifulSoup:
        tempURL = f"https://vsd.vn/vi/is?tab=5&page={pageIndex}"
        # Open the new URL in the same tab
        self.__driver.get(tempURL)
        time.sleep(0.5)
        # Get the page's source code
        html = self.__driver.page_source
        # Get the soup data from HTML using bs4
        soupData = BeautifulSoup(html, 'html5lib')
        # Get the table's soup
        tableSoup = soupData.find('table', id='tbListSymbol')
        return tableSoup

    def __getDataTupleFromOnePage(
            self,
            pageIndex: int
    ) -> list:
        time.sleep(0.1)
        tableSoup = self.__getTableSoupFromOnePage(pageIndex)
        # Get the data of beautiful soup data in each row
        rowLists = self.__getPageDataFromPageTableSoup(tableSoup, pageIndex)
        return rowLists

    @property
    def dfColName(
            self
    ) -> list:
        def processHeaders(header):
            return unidecode(header.text).title().replace(' ', '')

        # Get the xpath of the table's column numbers
        colNumXPath = '//*[@id="tbListSymbol"]//th'
        tableHeaders = self.__wait.until(EC.presence_of_all_elements_located((By.XPATH, colNumXPath)))
        return [processHeaders(header) for header in tableHeaders[1:]]

    # Client code
    def run(
            self
    ) -> None:
        self.__driver = self.driver
        # Get the xpath of the table's column numbers
        colNumXPath = '//*[@id="tbListSymbol"]//th'
        # Get the number of columns in the table
        self.__tableColNum = len(self.__wait.until(EC.presence_of_all_elements_located((By.XPATH, colNumXPath)))) - 1

        # The xpath of the button pointing to the last page ==> Later it will be used for getting the last page number
        lastPageXPath = "//*[text()='>>']//parent::button"
        # Get the element of last page
        lastPageElement = self.__wait.until(EC.presence_of_all_elements_located((By.XPATH, lastPageXPath)))[0]
        # Get the text of the element
        lastPageHTMLText = lastPageElement.get_attribute('onclick')
        lastPageNum = int(re.search(r'(\d+)', lastPageHTMLText).group())
        # Create a list of tuples by chaining all lists of each page sequentially
        # [('URL', 'MaCK', 'MaISIN', 'TenCK', 'LoaiCK', 'SanGiaoDich', 'NoiQuanLy', 'TrangThai'),...]
        dfData = chain(*map(self.__getDataTupleFromOnePage, range(1, lastPageNum + 1)))

        # Create a dataframe from dfData
        dfResult = DataFrame(
            data=dfData,
            columns=['URL'] + self.dfColName
        )

        # Swap the columns order to fit the current table on database
        dfResult[self.dfColName] = dfResult[
            ['MaCk', 'MaIsin', 'TenChungKhoan', 'LoaiChungKhoan', 'SanGiaoDich', 'NoiQuanLy', 'TrangThai']
        ]

        # Rename the columns
        dfResult.rename(
            columns={
                'MaCk': 'MaChungKhoan',
                'MaIsin': 'MaISIN'
            },
            inplace=True
        )

        # chạy trong ngày -> xem là số ngày hôm nay
        date = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # Insert column 'Ngay' for tracking the date of getting values
        dfResult.insert(0, 'Ngay', date)

        self.__driver.quit()

        # những mã có trạng thái hiệu lực
        tableActive = dfResult.loc[dfResult['TrangThai'] == 'Hiệu lực'].reset_index(drop=True)

        initSecuritiesInfo = SecuritiesInfoSendRequest()

        tableActive['HTML'] = None

        for idx in tableActive.index:
            url = tableActive.loc[idx, 'URL']
            initSecuritiesInfo.URL = url
            print(url)
            if tableActive.loc[idx, 'HTML'] == '':
                continue
            tableActive.loc[idx, 'HTML'] = initSecuritiesInfo.run

        # những mã có trạng thái Hủy đăng ký
        tableNotActive = dfResult.loc[dfResult['TrangThai'] != 'Hiệu lực'].reset_index(drop=True)
        tableNotActive['HTML'] = None
        # concat 2 bảng
        finalTable = pd.concat([tableActive, tableNotActive], axis=0).reset_index(drop=True)
        # Ghi database
        runDate = date
        dateString = runDate.strftime('%Y-%m-%d')

        DELETE(
            conn=connect_DWH_ThiTruong,
            table="DanhSachChungKhoanVSD",
            where=f"WHERE [Ngay] = '{dateString}'",
        )

        # Ghi database
        BATCHINSERT(
            conn=connect_DWH_ThiTruong,
            table='DanhSachChungKhoanVSD',
            df=finalTable,
        )


class SecuritiesInfoSendRequest:

    """
    Lấy nội dung HTML của mã
    - Dùng requests (trong trường hợp có ít mã -> gửi request được)
    """

    def __init__(self):
        self.__URL = None
        self.__session = None

    def __del__(self):
        self.session.close()

    @property
    def URL(self):
        return self.__URL

    @URL.setter
    def URL(self, value: str):
        self.__URL = value

    @property
    def session(self):
        if self.__session is not None:
            return self.__session

        self.__session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=10)
        self.__session.mount('https://', adapter)

        return self.__session

    @staticmethod
    def __cleanText(x):
        """
        Hàm xóa những kí tự dư thừa trong string của các giá trị trong cột Attribute
        """

        return re.sub(r'[!,*)@#:%(\'&$_?.^]', '', x).strip()

    def crawlContent(self, URL: str) -> list:
        """
        Hàm để crawl nội dung
        """

        response = self.session.get(
            url=URL,
            headers={'User-Agent': 'Chrome/100.0.4896.88'},
            timeout=30
        )
        soup = BeautifulSoup(response.content, 'html5lib')
        table = soup.find(class_='news-issuers')
        try:
            rows = table.findAll(name='div', class_='row')
        except(Exception,):
            rows = []
        return rows

    @property
    def run(self) -> str:

        """
        Chỗ này xử lý replace như bên dưới là vì URL gốc crawl từ VSD về có dạng là https://vsd.vn/s-detail/7353
        phải thêm vi hoặc en vào trước s-detail để crawl được dữ liệu cả tiếng Việt lẫn tiếng Anh
        """

        # record tiếng Việt
        url = self.__URL.replace('/s-detail', '/vi/s-detail')
        recordVi = self.crawlContent(url)
        url = self.__URL.replace('/s-detail', '/en/s-detail')
        # record tiếng Anh
        recordEn = self.crawlContent(url)

        tableContents = BeautifulSoup()
        tableContents.extend(recordVi + recordEn)

        return str(tableContents)



# class SecuritiesInfoMultipleProcessing:
#     """
#     Lấy nội dung HTML của mã
#     - Dùng multiple processing (trong trường hợp có quá nhiều mã -> ko thể gửi request được vì có thể bị block)
#     """
#
#     def __init__(
#             self,
#             urlSeries: pd.Series
#     ) -> None:
#         self.__urlSeries = urlSeries
#         self.__tableContents: BeautifulSoup
#         self.__numCPUs = self.__setupNumCPUs()
#
#     def __setupNumCPUs(
#             self
#     ) -> int:
#         # Calculate the available CPUs: Total CPUs - Used CPUs
#         # Use only 70% of the available CPUs left
#         self.__numCPUs = int(cpu_count() * (1 - cpu_percent() / 100) * 0.7)
#         if self.__numCPUs != 0:
#             print(f'\nAvailable CPUs: {self.__numCPUs}', end='\n\n')
#             return self.__numCPUs
#         errorContent = "Không đủ CPUs để thực hiện tác vụ, vui lòng đóng những ứng dụng, cửa sổ và tab không cần thiết!"
#         raise CrawlException(errorContent)
#
#     @staticmethod
#     def __initDriver() -> webdriver.Chrome:
#         option = Options()
#         option.headless = True
#         option.add_experimental_option('excludeSwitches', ['enable-logging'])
#         # Initialize the driver for Google Chrome
#         driver = webdriver.Chrome(
#             service=Service(PATH),
#             options=option
#         )
#         # Open the initial window web page
#         driver.get(f"https://vsd.vn/vi/is?tab=5&page=1")
#         return driver
#
#     def __getDataframeForOneTicker(
#             self,
#             driver: webdriver.Chrome,
#             url: str,
#     ) -> str:
#
#         def getTableElementsFromOnePage(
#                 lang: str
#         ) -> list:
#             # Open the url for the ticker's url
#             print(url)
#             driver.get(url.replace('/s-detail', f'/{lang}/s-detail'))
#             # Get the page source from the url
#             page = driver.page_source
#
#             # Convert the page source string into BeautifulSoup object
#             html = BeautifulSoup(page, 'html5lib')
#             # Find the table element from the url
#             tableElement = html.find(class_='news-issuers')
#
#             rowElements = tableElement.findAll(name='div', class_='row')
#             # Check if the found html missed information
#             timeoutIndex = 0
#             while not rowElements:
#                 timeoutIndex += 1
#                 # Raise timeout error if there are 10 consecutive failed crawls
#                 if timeoutIndex > 10:
#                     raise CrawlException("Không có nội dung trong HTML!")
#                 # Retry to get a list of row's Selenium elements
#                 rowElements = tableElement.findAll(name='div', class_='row')
#             return rowElements
#
#         # Get the list of data table from crawling in Vietnamese
#         vietnameseTable = getTableElementsFromOnePage(lang='vi')
#         # Get the list of data table from crawling in English
#         englishTable = getTableElementsFromOnePage(lang='en')
#
#         self.__tableContents = BeautifulSoup()
#         self.__tableContents.extend(vietnameseTable + englishTable)
#         # Convert HTML to string, since multiprocessing does not support Beautifulsoup type
#         return str(self.__tableContents)
#
#     def _crawlingData(
#             self,
#             crawlRange: range,
#             tableRowList: list
#     ):
#         # Initialize the driver for Google Chrome
#         driver = SecuritiesInfoMultipleProcessing.__initDriver()
#         for tickerIndex in crawlRange:
#             # Append a tuple (ticker's index, ticker's information dataframe) to the manager list of multiprocessing
#             tableRowList.append(
#                 (tickerIndex, self.__getDataframeForOneTicker(driver, self.__urlSeries[tickerIndex]))
#             )
#             time.sleep(0.2)
#
#     @property
#     def HTMLColumn(
#             self
#     ) -> list:
#
#         noTicker = len(self.__urlSeries)
#         # Get the list of elements from diving a number into n nearly equal parts
#         # For example: noTicker = 102, noProcess = 4 ==> [26, 26, 25, 25]
#         loopIterList = [
#             (noTicker // self.__numCPUs) + (1 if i < (noTicker % self.__numCPUs) else 0) for i in range(self.__numCPUs)
#         ]
#         print(loopIterList)
#
#         # Create a manager to store values when using multiprocessing
#         manager = Manager()
#         # A list to store dataframes of each ticker using multiprocessing
#         dfListRows = manager.list()
#
#         processes = []
#         for i in range(self.__numCPUs):
#             # Compute the startIndex for each process
#             startIndex = sum(loopIterList[:i]) if i != 0 else 0
#             # Create a process for parallel processing
#             process = Process(
#                 target=self._crawlingData,
#                 args=(range(startIndex, startIndex + loopIterList[i]), dfListRows)
#             )
#             # Start running a process
#             process.start()
#             # Append a process to the list
#             processes.append(process)
#
#         # Loop through each process to join them together for parallel processing
#         for process in processes:
#             # Join a process in parallel processing
#             process.join()
#
#         # Get second values: data for dataframes from the list of lists after sorting the list by indices
#         # Convert HTML to BeautifulSoup after multiprocessing
#         sortedResult = [x[1] for x in sorted(dfListRows)]
#
#         return sortedResult
