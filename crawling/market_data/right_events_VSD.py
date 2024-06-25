import requests
import bs4
from bs4 import BeautifulSoup
import datetime as dt
import pandas as pd
import time
import re
from os.path import dirname, join

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import ElementNotInteractableException

from request import connect_DWH_ThiTruong
from datawarehouse import BATCHINSERT, DELETE

class NoDataException(Exception,):
    pass


class MainPageCrawler:

    PATH = join(dirname(dirname(dirname(__file__))),'dependency','chromedriver')

    def __init__(self):
        
        self.__driver = None
        self.__wait = None
        self.__ignoredExceptions = (
            ValueError,
            IndexError,
            NoSuchElementException,
            StaleElementReferenceException,
            TimeoutException,
            ElementNotInteractableException
        )
        self.__colMapping = None
        self.__fromDate = None
        self.__toDate = None
        self.__page = None
        self.__totalPageNumbers = None
        self.__clearCache()

    def __del__(self):
        self.driver.quit()

    def __clearCache(self):
        self.__onpageURL = None
        self.__onpageNgayDangKyCuoiCung = None
        self.__onpageMaChungKhoan = None
        self.__onpageMaISIN = None
        self.__onpageTieuDe = None
        self.__onpageLoaiChungKhoan = None
        self.__onpageThiTruong = None
        self.__onpageNoiQuanLy = None

    @property
    def URL(self):
        return 'https://vsd.vn/vi/lich-giao-dich?tab=LICH_THQ&date=10/2022'

    @property
    def driver(self):
        if self.__driver is not None:
            return self.__driver
        self.__driver = webdriver.Chrome(executable_path=type(self).PATH)
        self.__driver.get(self.URL)
        self.__driver.maximize_window()
        return self.__driver
    
    @property
    def wait(self):
        self.__wait = WebDriverWait(
            self.driver,
            20,
            ignored_exceptions=self.__ignoredExceptions
        )
        return self.__wait

    @property
    def fromDate(self): # Ngày đăng ký cuối cùng
        return self.__fromDate

    @property
    def toDate(self): # Ngày đăng ký cuối cùng
        return self.__toDate

    def setPages(self,fromDate,toDate):
        # From date
        fromDateInput = self.wait.until(EC.presence_of_element_located((By.ID,'txtSearchLichTHQ_TuNgay')))
        fromDateInput.send_keys(Keys.CONTROL,'a',Keys.DELETE,fromDate.strftime('%d%m%Y'))
        # To date
        toDateInput = self.wait.until(EC.presence_of_element_located((By.ID,'txtSearchLichTHQ_DenNgay')))
        toDateInput.send_keys(Keys.CONTROL,'a',Keys.DELETE,toDate.strftime('%d%m%Y'))
        # Click Tìm kiếm
        xpath = "//*[contains(@onclick,'btnSearchLichTHQ()')]"
        searchButton = self.wait.until(EC.presence_of_element_located((By.XPATH,xpath)))
        searchButton.click()
        time.sleep(1)
        # Check có dữ liệu không
        totalRecordText = self.wait.until(EC.presence_of_element_located((By.ID,'d_total_rec'))).text
        if ':0-' in totalRecordText.replace(' ',''):
            raise NoDataException("No data to available")
        # Re-assign (sau khi kiểm tra có dữ liệu)
        self.__fromDate = fromDate
        self.__toDate = toDate
        self.__totalPageNumbers = None
        self.page = 1
        
    @property
    def page(self):
        return self.__page

    @page.setter
    def page(self,value):
        xpath = f"//*[@onclick='changePage_LichTHQ({value})']"
        pageButton = self.wait.until(EC.element_to_be_clickable((By.XPATH,xpath)))
        pageButton.click()
        time.sleep(0.5) # chờ animation
        self.__page = value
        self.__clearCache()

    @property
    def totalPageNumbers(self):
        if self.__totalPageNumbers is not None:
            return self.__totalPageNumbers
        xpath = "//*[text()='>>']//parent::button"
        lastPageButton = self.wait.until(EC.presence_of_element_located((By.XPATH,xpath)))
        return int(re.search(r'(\d+)',lastPageButton.get_attribute('onclick')).group())

    @property
    def _colMapping(self):
        if self.__colMapping is not None:
            return self.__colMapping
        xpath = '//*[@id="tblLichTHQ"]//th'
        headerElements = self.wait.until(EC.presence_of_all_elements_located((By.XPATH,xpath)))
        self.__colMapping = {
            element.get_attribute('data-sort'): num 
            for num, element in enumerate(headerElements, start=1)
        }
        return self.__colMapping

    @property
    def onpageURL(self):
        xpath = f"//*[@id='tblLichTHQ']/tbody/tr/td/a"
        elements = self.wait.until(EC.presence_of_all_elements_located((By.XPATH,xpath)))
        self.__onpageURL = [element.get_attribute('href') for element in elements]
        return self.__onpageURL

    @property
    def onpageNgayDangKyCuoiCung(self):
        position = self._colMapping['NgayDkcc']
        xpath = f"//*[@id='tblLichTHQ']/tbody/tr/td[position()={position}]"
        elements = self.wait.until(EC.presence_of_all_elements_located((By.XPATH,xpath)))
        self.__onpageNgayDangKyCuoiCung = [dt.datetime.strptime(element.text,'%d/%m/%Y') for element in elements]
        return self.__onpageNgayDangKyCuoiCung

    @property
    def onpageMaChungKhoan(self):
        position = self._colMapping['StockCode']
        xpath = f"//*[@id='tblLichTHQ']/tbody/tr/td[position()={position}]"
        elements = self.wait.until(EC.presence_of_all_elements_located((By.XPATH,xpath)))
        self.__onpageMaChungKhoan = [element.text for element in elements]
        return self.__onpageMaChungKhoan

    @property
    def onpageISIN(self):
        position = self._colMapping['IsinCode']
        xpath = f"//*[@id='tblLichTHQ']/tbody/tr/td[position()={position}]"
        elements = self.wait.until(EC.presence_of_all_elements_located((By.XPATH,xpath)))
        self.__onpageMaISIN = [element.text for element in elements]
        return self.__onpageMaISIN

    @property
    def onpageTieuDe(self):
        position = self._colMapping['Title']
        xpath = f"//*[@id='tblLichTHQ']/tbody/tr/td[position()={position}]"
        elements = self.wait.until(EC.presence_of_all_elements_located((By.XPATH,xpath)))
        self.__onpageTieuDe = [element.text for element in elements]
        return self.__onpageTieuDe
           
    @property
    def onpageLoaiChungKhoan(self):
        position = self._colMapping['StockTypeName']
        xpath = f"//*[@id='tblLichTHQ']/tbody/tr/td[position()={position}]"
        elements = self.wait.until(EC.presence_of_all_elements_located((By.XPATH,xpath)))
        self.__onpageLoaiChungKhoan = [element.text for element in elements]
        return self.__onpageLoaiChungKhoan

    @property
    def onpageThiTruong(self):
        position = self._colMapping['MarketName']
        xpath = f"//*[@id='tblLichTHQ']/tbody/tr/td[position()={position}]"
        elements = self.wait.until(EC.presence_of_all_elements_located((By.XPATH,xpath)))
        self.__onpageThiTruong = [element.text for element in elements]
        return self.__onpageThiTruong

    @property
    def onpageNoiQuanLy(self):
        position = self._colMapping['ManagementAreaName']
        xpath = f"//*[@id='tblLichTHQ']/tbody/tr/td[position()={position}]"
        elements = self.wait.until(EC.presence_of_all_elements_located((By.XPATH,xpath)))
        self.__onpageNoiQuanLy = [element.text for element in elements]
        return self.__onpageNoiQuanLy

    @property
    def onpageTable(self):        
        return pd.DataFrame(
            data = {
                'URL': self.onpageURL,
                'NgayDangKyCuoiCung': self.onpageNgayDangKyCuoiCung,
                'MaChungKhoan': self.onpageMaChungKhoan,
                'MaISIN': self.onpageISIN,
                'TieuDe': self.onpageTieuDe,
                'LoaiChungKhoan': self.onpageLoaiChungKhoan,
                'ThiTruong' : self.onpageThiTruong,
                'NoiQuanLy': self.onpageNoiQuanLy,
            }
        )

    def crawl(self):

        frames = []
        for page in range(1, self.totalPageNumbers + 1):
            self.page = page
            frames.append(self.onpageTable)

        return pd.concat(frames).reset_index(drop=True)


class ArticleCrawler:

    def __init__(self):

        self.__URL = None
        self.__session = None

    def __del__(self):
        self.session.close()

    @property
    def session(self):

        if self.__session is not None:
            return self.__session

        self.__session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=10)
        self.__session.mount('https://vsd.vn/',adapter)

        return self.__session

    @property
    def URL(self):
        return self.__URL
    
    @URL.setter
    def URL(self,value):
        self.__URL = value
    
    @staticmethod
    def __parseTime(tag: bs4.element.Tag):
        rawText = tag.text
        processedText = re.sub('\D','',rawText)
        return dt.datetime.strptime(processedText,'%d%m%Y%H%M%S')
    
    @staticmethod
    def __parseBody(tag: bs4.element.Tag):
        return tag.prettify()

    def crawl(self):
        
        response = self.session.get(
            url = self.URL, 
            headers = {'User-Agent':'Chrome/100.0.4896.88'}, 
            timeout = 10
        )
        soup = BeautifulSoup(response.text,'html5lib')
        # Time
        timeTag = soup.find(class_='time-newstcph')
        time = type(self).__parseTime(timeTag)
        # Body
        bodyTag = soup.find(class_='content-category')
        body = type(self).__parseBody(bodyTag)

        return {'ThoiGian': time, 'NoiDungHTML': body}


# Client code
def run(fromDate,toDate):

    """
    fromDate, toDate là ngày đăng ký cuối cùng
    """
    
    # Khởi tạo các object
    mainPage = MainPageCrawler()
    mainPage.setPages(fromDate,toDate)
    article = ArticleCrawler()
    
    # Crawl bảng các article
    resultTable = mainPage.crawl()
    del mainPage

    # Crawl nội dung
    resultTable[['ThoiGian','NoiDungHTML']] = None        
    for index in resultTable.index:
        article.URL = resultTable.loc[index,'URL']
        articleContent = article.crawl()
        resultTable.loc[index,'ThoiGian'] = articleContent.get('ThoiGian')
        resultTable.loc[index,'NoiDungHTML'] = articleContent.get('NoiDungHTML')
    
    # Ghi database
    fromDateString = fromDate.strftime('%Y-%m-%d 00:00:00')
    toDateString = toDate.strftime('%Y-%m-%d 23:59:59')
    DELETE(
        conn = connect_DWH_ThiTruong,
        table = "TinThucHienQuyenVSD",
        where = f"WHERE [NgayDangKyCuoiCung] BETWEEN '{fromDateString}' AND '{toDateString}'",
    )
    BATCHINSERT(
        conn = connect_DWH_ThiTruong,
        table = 'TinThucHienQuyenVSD',
        df = resultTable,
    )
    

if __name__ == '__main__':
    run(dt.datetime(2022,12,7),dt.datetime(2022,12,29))


