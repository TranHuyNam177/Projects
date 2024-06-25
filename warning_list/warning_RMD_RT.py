"""
Cú pháp: run(True) hoặc run(False)  |  True: Ẩn browser window, False Không ẩn browser window
"""

import time
import numpy as np
import pandas as pd
import datetime as dt
from os.path import join, dirname, realpath, isfile
from os import listdir, remove
from win32com.client import Dispatch
from function import bdate
from request import connect_DWH_CoSo
from request.stock import internal, ta
from news_collector import scrape_ticker_by_exchange

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


def __getForceSell__(d):
    return pd.read_sql(
        f"""
        WITH 
        [t] AS (
            SELECT
                [sub_account].[account_code],
                [vmr9004].[ticker],
                SUM([vmr9004].[total_volume]) [volume]
            FROM [vmr9004] LEFT JOIN [sub_account] 
            ON [vmr9004].[sub_account] = [sub_account].[sub_account] AND [vmr9004].[date] = '{d}'
            WHERE 
                EXISTS (
                    SELECT [vmr0003].[sub_account] FROM [vmr0003] 
                    WHERE [vmr0003].[sub_account] = [vmr9004].[sub_account]
                        AND [vmr0003].[date] = '{d}'
                    ) 
                -- loại các tk nợ xấu, tự doanh
                AND [sub_account].[account_code] NOT IN (
                    '022E002222',
                    '022P002222',
                    '022C006827',
                    '022C012621',
                    '022C012620',
                    '022C012622',
                    '022C089535',
                    '022C050302',
                    '022C089950',
                    '022C089957'
                )
            GROUP BY [account_code], [ticker]
        )
        SELECT 
            [t].*,
            CONCAT([t].[account_code],' (',CAST([t].[volume] AS BIGINT),')') [combine]
        FROM [t]
        """,
        connect_DWH_CoSo
    )


def __formatPrice__(x):
    if not x:  # x == ''
        x = np.nan
    elif x in ('ATO', 'ATC'):
        x = np.nan
    else:
        x = float(x)
    return x


def __formatVolume__(x):
    if not x:  # x == ''
        x = np.nan
    else:
        x = float(x.replace(',', '')) * 10  # 2,30 -> 2300, 10 -> 100
    return x


def __SendMailRetry__(func):  # Decorator

    def wrapper(*args, **kwargs):
        n = 0
        while True:
            now = dt.datetime.now()
            # set các điều kiện break:
            if now.time() > dt.time(15, 0, 0):
                tempFiles = listdir(join(dirname(__file__), 'TempFiles'))
                trashFiles = [f for f in tempFiles if not f.startswith('README')]
                for file in trashFiles:
                    remove(join(dirname(__file__), 'TempFiles', file))
                break
            if dt.time(11, 30, 0) < now.time() < dt.time(13, 0, 0):
                break
            if now.time() < dt.time(9, 0, 0):
                break

            """
            Quét HOSE trước
            Quét HNX sau
            """
            WarningsHOSE = func('HOSE', *args, **kwargs)  # quét HOSE trước
            print(WarningsHOSE.set_index('Ticker'))
            WarningsHNX = func('HNX', *args, **kwargs)  # quét HNX sau
            print(WarningsHNX.set_index('Ticker'))
            Warnings = pd.concat([WarningsHOSE, WarningsHNX])

            if Warnings.empty:  # Khi không có Warnings
                print(f"No warnings at {now.strftime('%H:%M:%S %d.%m.%Y')}")
                time.sleep(8 * 60)  # sleep 8 phút
                continue

            # Khi có Warnings
            WarningsHTML = Warnings.to_html(index=False, header=True).replace("\\n", "<br>")
            WarningsHTML = WarningsHTML.replace(
                'border="1"',
                'style="border-collapse:collapse; border-spacing:0px;"',
            )  # remove borders

            html_str = f"""
            <html>
                <head></head>
                <body>
                    {WarningsHTML}
                    <p style="font-family:Times New Roman; font-size:90%"><i>
                        -- Generated by Reporting System
                    </i></p>
                </body>
            </html>
            """

            outlook = Dispatch('outlook.application')
            mail = outlook.CreateItem(0)
            mail.To = 'anhnguyenthu@phs.vn;anhhoang@phs.vn;huyhuynh@phs.vn;phuhuynh@phs.vn'
            mail.CC = 'namtran@phs.vn'
            mail.Subject = f"Market Alert {now.strftime('%H:%M:%S %d.%m.%Y')}"
            mail.HTMLBody = html_str
            mail.Send()

            n += 1
            print('Quét lần: ', n)
            time.sleep(5 * 60)  # nghỉ 5 phút

    return wrapper


@__SendMailRetry__
def run(
        exchange: str,
        hide_window=True  # nên set=True để ko bị pop up cửa sổ browser liên tục trong phiên
) -> pd.DataFrame:
    print(exchange, hide_window)
    today = dt.datetime.now().strftime('%Y-%m-%d')
    forceSaleDate = bdate(today, -1)  # lấy danh sách force sell ngày hôm trước
    # get Force Sell table
    ForceSellFile = join(dirname(__file__), 'TempFiles', f"ForceSell_{forceSaleDate.replace('-', '.')}.pickle")
    if not isfile(ForceSellFile):
        ForceSellTable = __getForceSell__(forceSaleDate)
        ForceSellTable.to_pickle(ForceSellFile)
    else:
        ForceSellTable = pd.read_pickle(ForceSellFile)
    tickersForceSell = ForceSellTable['ticker'].unique()

    PATH = join(dirname(dirname(realpath(__file__))), 'dependency', 'chromedriver')
    ignored_exceptions = (
        ValueError,
        IndexError,
        NoSuchElementException,
        StaleElementReferenceException,
        TimeoutException,
        ElementNotInteractableException
    )

    start = time.time()

    # Lấy tickers
    tickersFile = join(dirname(__file__), 'TempFiles', f"TickerList_{today.replace('-', '.')}.pickle")
    if not isfile(tickersFile):
        allTickers = scrape_ticker_by_exchange.run().reset_index()
        allTickers.to_pickle(tickersFile)
    else:
        allTickers = pd.read_pickle(tickersFile)

    options = Options()
    if hide_window:
        options.headless = True
    driver = webdriver.Chrome(executable_path=PATH, options=options)
    wait = WebDriverWait(driver, 60, ignored_exceptions=ignored_exceptions)
    url = r'https://priceboard.vcbs.com.vn/Priceboard/'
    driver.get(url)
    time.sleep(5)  # bắt buộc

    allTickers = allTickers.loc[allTickers['ticker'].map(len) == 3]
    if exchange == 'HOSE':
        exchangeXpath = '//*[text()="HOSE"]'
        fullTickers = allTickers.loc[allTickers['exchange'] == 'HOSE', 'ticker']
        mlist = internal.mlist(['HOSE'])
    elif exchange == 'HNX':
        exchangeXpath = '//*[text()="HNX"]'
        fullTickers = allTickers.loc[allTickers['exchange'] == 'HNX', 'ticker']
        mlist = internal.mlist(['HNX'])
    else:
        raise ValueError('Currently monitor HOSE and HNX only')

    tickerPool = set(fullTickers) & (set(mlist) | set(tickersForceSell))  # các cp phải quét

    # produce/get avgVolume File
    avgVolumeFile = join(dirname(__file__), 'TempFiles', f"{exchange}_AvgPrice_{today.replace('-', '.')}.pickle")
    if not isfile(avgVolumeFile):
        fromdate = bdate(today, -22)
        avgVolume = pd.Series(index=tickerPool, dtype=object)
        for ticker in tickerPool:
            avgVolume.loc[ticker] = ta.hist(ticker, fromdate, today)['total_volume'].mean()
        avgVolume.to_pickle(avgVolumeFile)
    else:
        avgVolume = pd.read_pickle(avgVolumeFile)

    while True:
        try:
            driver.find_element(By.XPATH, exchangeXpath).click()
            break
        except (Exception,):
            time.sleep(1)

    warnings = pd.DataFrame(columns=['Marginable', 'Message'], index=pd.Index(tickerPool, name='Ticker'))
    for ticker in warnings.index:
        tickerElement = wait.until(EC.presence_of_element_located((By.XPATH, f'//tbody/*[@name="{ticker}"]')))
        sub_wait = WebDriverWait(tickerElement, 60, ignored_exceptions=ignored_exceptions)
        Floor = __formatPrice__(sub_wait.until(EC.presence_of_element_located((By.ID, f'{ticker}floor'))).text)
        MatchPrice = __formatPrice__(
            sub_wait.until(EC.presence_of_element_located((By.ID, f'{ticker}closePrice'))).text)
        SellVolume1 = __formatVolume__(
            sub_wait.until(EC.presence_of_element_located((By.ID, f'{ticker}best1OfferVolume'))).text)
        SellVolume2 = __formatVolume__(
            sub_wait.until(EC.presence_of_element_located((By.ID, f'{ticker}best2OfferVolume'))).text)
        SellVolume3 = __formatVolume__(
            sub_wait.until(EC.presence_of_element_located((By.ID, f'{ticker}best3OfferVolume'))).text)
        FrgBuy = __formatVolume__(sub_wait.until(EC.presence_of_element_located((By.ID, f'{ticker}foreignBuy'))).text)
        FrgSell = __formatVolume__(sub_wait.until(EC.presence_of_element_located((By.ID, f'{ticker}foreignSell'))).text)
        if MatchPrice == Floor:
            messages = []
            # Điều kiện 0:
            if ticker in mlist:
                warnings.loc[ticker, 'Marginable'] = ': Yes'
            else:
                warnings.loc[ticker, 'Marginable'] = ': No'
            # Điều kiện 1:
            if SellVolume1 + SellVolume2 + SellVolume3 >= 0.2 * avgVolume.loc[ticker]:
                messages.append('-- Giảm sàn, Dư bán hơn 20% KLTB')
            # Điều kiện 2:
            if FrgSell - FrgBuy >= 0.2 * avgVolume.loc[ticker]:
                messages.append('-- Giảm sàn, NN bán hơn 20% KLTB')
            # Điều kiện 3:
            if ticker in tickersForceSell:
                result = ForceSellTable.loc[ForceSellTable['ticker'] == ticker, 'combine']
                messages.append(f"-- Tài khoản force sell đang nắm giữ: {', '.join(result)}")
            warnings.loc[ticker, 'Message'] = '\n'.join(messages)

    warnings = warnings.loc[warnings['Message'] != '']
    warnings = warnings.dropna().reset_index()
    driver.quit()

    if __name__ == '__main__':
        print(f"{__file__.split('/')[-1].replace('.py', '')}::: Finished")
    else:
        print(f"{__name__.split('.')[-1]} ::: Finished")
    print(f'Total Run Time ::: {np.round(time.time() - start, 1)}s')

    return warnings
