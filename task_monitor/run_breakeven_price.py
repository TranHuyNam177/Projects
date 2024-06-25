from os.path import join, dirname, realpath
import time
import csv
import pandas as pd
import datetime as dt
import shutil
from request.stock import fa
from breakeven_price import monte_carlo_test
from implementation import TaskMonitor
from request import connect_DWH_CoSo
from datawarehouse import BATCHINSERT, DELETE, DROPDUPLICATES

check = '2023q3'

def BreakevenPriceInsertToDB(filePath: str, pValues: int):
    table = pd.read_csv(filePath)
    table = table.rename(
        {
            '0% at Risk': 'LowestBP',
            'Breakeven Price': 'BreakevenPrice'
        },
        axis=1
    )
    runDate = dt.datetime.now()
    runDateString = runDate.strftime("%Y-%m-%d")

    table['Date'] = runDate.replace(hour=0, minute=0, second=0, microsecond=0)
    table['pValues'] = pValues
    if runDate.weekday() == 2:
        DELETE(
            conn=connect_DWH_CoSo,
            table='BreakevenPrice',
            where=f"WHERE [Date] < '{runDateString}' AND [pValues] = 5"

        )
    BATCHINSERT(
        conn=connect_DWH_CoSo,
        table='BreakevenPrice',
        df=table
    )

# @TaskMonitor
def BreakevenPrice_5pct(  # run on Wed and Fri weekly
        tickers: list = None,
        exchanges: list = None,
):
    start_time = time.time()

    # destination_dir_github = join(dirname(dirname(realpath(__file__))), 'breakeven_price', 'result_table')
    # destination_dir_network = r'\\192.168.10.28\images\breakeven'
    # destination_dir_rmd = r'C:\Shared Folder\Risk Management\Gia Hoa Von
    # chart_folder = join(realpath(dirname(dirname(__file__))), 'breakeven_price', 'result_chart')

    # PATH to test
    destination_dir_github = join(dirname(dirname(realpath(__file__))), 'breakeven_price', 'result_table', check)
    destination_dir_network = fr'C:\Users\namtran\Shared Folder\Risk Management\images\breakeven\{check}'
    destination_dir_rmd = fr'C:\Users\namtran\Shared Folder\Risk Management\Gia Hoa Von\{check}'
    chart_folder = join(realpath(dirname(dirname(__file__))), 'breakeven_price', 'result_chart', check)

    if exchanges == 'all' or tickers == 'all':
        tickers = fa.tickers(exchange='all')
    # elif exchanges is not None and exchanges!='all':
    #     tickers = []
    #     for exchange in exchanges:
    #         tickers += fa.tickers(exchange=exchange)

    # update thêm mã từ file - 04/01/2024
    elif exchanges is not None and exchanges != 'all':
        tickers = []
        for exchange in exchanges:
            tickers += fa.tickers(exchange=exchange)
            update_file = r"C:\Shared Folder\Risk Management\UpdateStockBreakEven\Stock Update.xlsx"  # local
            update_table = pd.read_excel(
                update_file,
                sheet_name=f'{exchange}',
                usecols='A'
            )
            update_stock_list = update_table['Stock'].tolist()
            tickers += update_stock_list
    elif tickers is not None and tickers != 'all':
        pass

    network_table_path = join(destination_dir_network, 'tables', 'result.csv')
    table = pd.DataFrame(columns=['ticker', 'Breakeven Price'])
    table.set_index(keys=['ticker'], inplace=True)
    table.to_csv(network_table_path)

    now = dt.datetime.now()
    github_file_name = f'{now.day}.{now.month}.{now.year}.csv'
    github_table_path = join(destination_dir_github, github_file_name)
    table = pd.DataFrame(
        columns=['Ticker', '0% at Risk', '1% at Risk', '3% at Risk', '5% at Risk', 'Group', 'Breakeven Price']
    )
    table.set_index(keys=['Ticker'], inplace=True)
    table.to_csv(github_table_path)

    rmd_file_name = f'{now.day}.{now.month}.{now.year}.csv'
    rmd_table_path = join(destination_dir_rmd, rmd_file_name)
    table = pd.DataFrame(
        columns=['Ticker', 'Group', 'Breakeven Price', '0% at Risk']
    )
    table.set_index(keys=['Ticker'], inplace=True)
    table.to_csv(rmd_table_path)

    for ticker in tickers:
        try:
            lv0_price, lv1_price, lv2_price, lv3_price, breakeven_price, group = monte_carlo_test.run(ticker=ticker, alpha=0.05)
            with open(github_table_path, mode='a', newline='') as github_file:
                github_writer = csv.writer(github_file, delimiter=',')
                github_writer.writerow([ticker, lv0_price, lv1_price, lv2_price, lv3_price, group, breakeven_price])
            with open(network_table_path, mode='a', newline='') as network_file:
                network_writer = csv.writer(network_file, delimiter=',')
                network_writer.writerow([ticker, breakeven_price])
            with open(rmd_table_path, mode='a', newline='') as rmd_file:
                rmd_writer = csv.writer(rmd_file, delimiter=',')
                rmd_writer.writerow([ticker, group, breakeven_price, lv0_price])
        except (ValueError, KeyError, IndexError):
            print(f'{ticker} cannot be simulated')
            print('-------')
        try:
            shutil.copy(join(chart_folder, f'{ticker}.png'), join(realpath(destination_dir_network), 'charts'))
        except FileNotFoundError:
            print(f'{ticker} cannot be graphed')

        print('===========================')

    print('Finished!')
    print("Total execution time is: %s seconds" % (time.time() - start_time))
    # BreakevenPriceInsertToDB(filePath=rmd_table_path, pValues=5)
    # DROPDUPLICATES(connect_DWH_CoSo, 'BreakevenPrice', 'Ticker', 'Group', 'BreakevenPrice', 'LowestBP', 'pValues', 'Date')


# @TaskMonitor
def BreakevenPrice_2pct(  # weekly run as requested by RMD
        tickers: list = None,
        exchanges: list = None,
):
    start_time = time.time()

    # destination_dir_github = join(dirname(dirname(realpath(__file__))), 'breakeven_price', 'result_table')
    # destination_dir_rmd = r'C:\Shared Folder\Risk Management\Gia Hoa Von'

    # PATH to test
    destination_dir_github = join(dirname(dirname(realpath(__file__))), 'breakeven_price', 'result_table', check)
    destination_dir_rmd = fr'C:\Users\namtran\Shared Folder\Risk Management\Gia Hoa Von\{check}'

    if exchanges == 'all' or tickers == 'all':
        tickers = fa.tickers(exchange='all')

    # elif exchanges is not None and exchanges!='all':
    #     tickers = []
    #     for exchange in exchanges:
    #         tickers += fa.tickers(exchange=exchange)

    # update thêm mã từ file - 04/01/2024
    elif exchanges is not None and exchanges != 'all':
        tickers = []
        for exchange in exchanges:
            tickers += fa.tickers(exchange=exchange)
            update_file = r"C:\Shared Folder\Risk Management\UpdateStockBreakEven\Stock Update.xlsx"  # local
            update_table = pd.read_excel(
                update_file,
                sheet_name=f'{exchange}',
                usecols='A'
            )
            update_stock_list = update_table['Stock'].tolist()
            tickers += update_stock_list
    elif tickers is not None and tickers != 'all':
        pass

    now = dt.datetime.now()
    github_file_name = f'{now.day}.{now.month}.{now.year}_0.02.csv'
    github_table_path = join(destination_dir_github, github_file_name)
    table = pd.DataFrame(
        columns=['Ticker', '0% at Risk', '1% at Risk', '3% at Risk', '5% at Risk', 'Group', 'Breakeven Price'])
    table.set_index(keys=['Ticker'], inplace=True)
    table.to_csv(github_table_path)

    rmd_file_name = f'{now.day}.{now.month}.{now.year}_0.02.csv'
    rmd_table_path = join(destination_dir_rmd, rmd_file_name)
    table = pd.DataFrame(
        columns=['Ticker', 'Group', 'Breakeven Price', '0% at Risk']
    )
    table.set_index(keys=['Ticker'], inplace=True)
    table.to_csv(rmd_table_path)

    for ticker in tickers:
        try:
            lv0_price, lv1_price, lv2_price, lv3_price, breakeven_price, group = monte_carlo_test.run(ticker=ticker, alpha=0.02)
            with open(github_table_path, mode='a', newline='') as github_file:
                github_writer = csv.writer(github_file, delimiter=',')
                github_writer.writerow([ticker, lv0_price, lv1_price, lv2_price, lv3_price, group, breakeven_price])
            with open(rmd_table_path, mode='a', newline='') as rmd_file:
                rmd_writer = csv.writer(rmd_file, delimiter=',')
                rmd_writer.writerow([ticker, group, breakeven_price, lv0_price])
        except (ValueError, KeyError, IndexError):
            print(f'{ticker} cannot be simulated')
            print('-------')

        print('===========================')

    print('Finished!')
    print("Total execution time is: %s seconds" % (time.time() - start_time))
    # BreakevenPriceInsertToDB(filePath=rmd_table_path, pValues=2)
    # DROPDUPLICATES(connect_DWH_CoSo, 'BreakevenPrice', 'Ticker', 'Group', 'BreakevenPrice', 'LowestBP', 'pValues', 'Date')