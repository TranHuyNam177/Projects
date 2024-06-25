from os.path import dirname, join
import time
import datetime as dt
import re
import pandas as pd
from request import connect_DWH_ThiTruong
from datawarehouse import DELETE, BATCHINSERT
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


def run(
    fromDate: dt.datetime,
    toDate: dt.datetime,
):

    fromDate = fromDate.replace(hour=0,minute=0,second=0,microsecond=0)
    toDate = toDate.replace(hour=0,minute=0,second=0,microsecond=0)
    PATH = join(dirname(dirname(dirname(__file__))),'dependency','chromedriver')
    ignored_exceptions = (
        ValueError,
        IndexError,
        NoSuchElementException,
        StaleElementReferenceException,
        TimeoutException,
        ElementNotInteractableException
    )
    options = Options()
    options.headless = False
    driver = webdriver.Chrome(options=options,executable_path=PATH)
    driver.maximize_window()
    wait = WebDriverWait(driver,10,ignored_exceptions=ignored_exceptions)

    URL = r'https://www.vsd.vn/vi/thong-tin-san-pham'
    driver.get(URL)

    # Click tab "Giá thanh toán cuối ngày (DSP), giá thanh toán cuối cùng (FSP)"
    xpath = "//a[contains(text(),'Giá thanh toán cuối ngày')]"
    tabElement = wait.until(EC.presence_of_element_located((By.XPATH,xpath)))
    tabElement.click()
    # Điền ngày
    xpath = "//input[contains(@id,'txtSearchPriceDate')]"
    dateBox = wait.until(EC.presence_of_element_located((By.XPATH,xpath)))
    xpath = "//button[contains(@class,'btn-sl-search')]"
    searchButton = wait.until(EC.presence_of_element_located((By.XPATH,xpath)))

    records = []
    for d in pd.date_range(fromDate,toDate):
        # Clear ô ngày
        dateBox.clear()
        # Ngày ngày
        dateString = d.strftime('%d%m%Y')
        dateBox.click()
        for _ in range(10):
            dateBox.send_keys(Keys.BACKSPACE)
        dateBox.send_keys(dateString)
        # Bấm search
        searchButton.click()
        time.sleep(2) # chờ load data
        # Lấy record
        xpath = "//*[@id='divTab_Fill_GiaThanhToan']//tbody/tr"
        rowElements = driver.find_elements(By.XPATH,xpath)
        if not rowElements: # ngày này không có data
            continue
        rowStrings = [element.text for element in rowElements]
        for rowString in rowStrings:
            _, ticker, paymentDateString, priceString, _ = rowString.split()
            paymentDate = dt.datetime.strptime(paymentDateString,'%d/%m/%Y')
            if priceString == '-':
                price = 0
            else:
                price = float(priceString.replace('.','').replace(',','.'))
            records.append((d,ticker,paymentDate,price))

    driver.quit()
    if not records: # không có record
        print('No data to insert')
        return

    table = pd.DataFrame(records,columns=['Ngay','MaSanPham','NgayThanhToan','GiaThanhToanNgayDSP'])
    # Insert vào Database
    fromDateString = fromDate.strftime('%Y-%m-%d')
    toDateString = toDate.strftime('%Y-%m-%d')
    whereClause = f"WHERE [Ngay] BETWEEN '{fromDateString}' AND '{toDateString}'"
    DELETE(connect_DWH_ThiTruong,"GiaThanhToanPhaiSinhVSD",whereClause)
    BATCHINSERT(connect_DWH_ThiTruong,'GiaThanhToanPhaiSinhVSD',table)

