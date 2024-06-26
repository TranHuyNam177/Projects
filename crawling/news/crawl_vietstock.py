from os.path import join, dirname, realpath
from os import listdir, remove
import pandas as pd
import time
import os

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait,Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service


PATH = join(dirname(dirname(realpath(__file__))),'dependency','chromedriver')
ignored_exceptions = (
    ValueError,
    IndexError,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    ElementNotInteractableException
)


def run(  # this function can't go headless
    run_date:str,
) -> pd.DataFrame:

    day = run_date[-2:]
    month = run_date[5:7]
    year = run_date[:4]

    url = r'https://finance.vietstock.vn/ket-qua-giao-dich?tab=thong-ke-gia&exchange=1'
    driver = webdriver.Chrome(service=Service(PATH))
    wait = WebDriverWait(driver,10,ignored_exceptions=ignored_exceptions)
    driver.get(url)
    driver.maximize_window()

    # Đăng nhập
    login_element = driver.find_element(By.XPATH, "//*[contains(@class, 'title-link btnlogin')]")
    login_element.click()
    email_element = driver.find_element(By.XPATH, '//*[@id="txtEmailLogin"]')
    email_element.clear()
    email_element.send_keys('hiepdang@phs.vn')
    password_element = driver.find_element(By.XPATH, '//*[@id="txtPassword"]')
    password_element.clear()
    password_element.send_keys('123456')
    login_element = driver.find_element(By.XPATH,'//*[@id="btnLoginAccount"]')
    login_element.click()

    # Lấy dữ liệu 3 sàn
    for exchange in ['HOSE','HNX','UPCoM']:
        time.sleep(5)
        select_exchange = Select(driver.find_element(
            By.XPATH,
            '//*[@id="trading-result"]/div/div[1]/div[1]/div/div[1]/div/div[1]/div/select'
        ))
        select_exchange.select_by_visible_text(exchange)
        select_fromdate = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="txtFromDate"]/input')))
        select_fromdate.clear()
        select_fromdate.send_keys(f'{day}/{month}/{year}')
        select_todate = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="txtToDate"]/input')))
        select_todate.clear()
        select_todate.send_keys(f'{day}/{month}/{year}')
        excel_export_element = wait.until(EC.presence_of_element_located(
            (By.XPATH,'//*[@title="Export Excel"]')
        ))
        while True:
            try:
                excel_export_element.click()
                break
            except (Exception,):
                continue

    time.sleep(1)
    driver.quit()

    # download_folder = r'C:\Users\namtran\Downloads'
    download_folder = fr'C:\Users\{os.getlogin()}\Downloads'
    files = pd.DataFrame(
        [f for f in listdir(download_folder) if f'{year}{month}{day}' in f and 'KQGD' in f],
        columns=['raw'],
    )
    # in case many files already there
    files['exchange'] = files['raw'].str.split('-').str.get(1)
    files['d'] = files['raw'].str.split('-').str.get(3)
    files['s'] = files['raw'].str.split('-').str.get(4)
    files['t'] = files['raw'].str.split('-').str.get(5)
    files.sort_values(['d','s','t'],ascending=False,inplace=True)
    files = files.head(3)['raw']
    tables = []
    for file in files:
        exchange = file.split('-')[1]
        ticker_col = 'Mã'
        n_skip_rows = 14
        # theo rule của DVKH
        if exchange in ['HSX','HNX']:
            price_col = 'Đóng\n cửa'
        else:
            price_col = 'Trung\n bình'
        frame = pd.read_excel(join(download_folder,file),skiprows=n_skip_rows,header=0)
        frame = frame[[ticker_col,price_col]].copy()
        frame.dropna(subset=[ticker_col],inplace=True)
        frame.columns = ['ticker','price']
        frame.set_index('ticker',inplace=True)
        frame.insert(0,'exchange',exchange)
        frame['price'] = frame['price']*1000
        tables.append(frame)

        # delete downloaded files
        remove(join(download_folder,file))

    result = pd.concat(tables)
    result = result.reset_index()
    result = result.drop_duplicates(subset=['ticker', 'exchange'])
    result = result.set_index('ticker')

    return result
