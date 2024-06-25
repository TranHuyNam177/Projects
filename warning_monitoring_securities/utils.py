import json
import re
import os
import pandas as pd
import datetime as dt

from os import listdir
from os.path import join


class InputDataError(Exception):
    pass


class PATH:

    # path = join(os.environ["PYTHONPATH"], 'warning', 'MonitorStock', 'path.json')  # server
    path = r"D:\DataAnalytics\warning\MonitorStock\path.json"
    with open(path, mode='r', encoding='utf-8') as __file:
        pattern = json.load(__file)  # dictionary


class Check:

    @staticmethod
    def note1(
            marketCap,
            lastDayVolume
    ) -> str:

        noteList = []
        if marketCap <= 100e9:
            noteList.append('MC ≤ 100bn')
        if lastDayVolume <= 5000:
            noteList.append('AV ≤ 5K')
        elif 5000 < lastDayVolume <= 10000:
            noteList.append('5K < AV ≤ 10K')
        elif lastDayVolume > 10000:
            noteList.append('AV > 10K')
        noteString = ', '.join(noteList)
        return noteString

    @staticmethod
    def note2(
            currentBP,
            BP_05,
            BP_130perRP
    ) -> str:

        noteList = []
        if currentBP < BP_05:
            noteList.append('CBP < Var.')
        elif currentBP > BP_05:
            noteList.append('CBP > Var.')
        if BP_130perRP > BP_05:
            noteList.append('BP of 130% > Var.')
        noteString = ', '.join(noteList)
        return noteString


class ReadDataFromExcel:

    def __init__(self):
        self.__path = None
        self.__result = None

    @property
    def fixedMP(self) -> pd.DataFrame:

        self.__path = fr"{PATH.pattern['PATH'].get('FixedMP')}"
        self.__result = pd.read_excel(
            join(self.__path, 'Fixed MP List.xlsx'),
            sheet_name='Sheet1',
            usecols='B'
        )
        return self.__result

    @staticmethod
    def convertStringToDateTime(dateString: str, dateFormat: str) -> dt.datetime:
        return dt.datetime.strptime(dateString, dateFormat)

    @property
    def Intranet(self) -> pd.DataFrame:

        now = dt.datetime.now()
        self.__path = join(fr"{PATH.pattern['PATH'].get('Intranet')}", str(now.year))
        allFiles = [
            file
            for file in listdir(self.__path)
            if '~$' not in file
        ]
        allTimesInFile = [
            self.convertStringToDateTime(re.search(r'\d{1,2}\.\d{1,2}\.\d{4}', file).group(), "%d.%m.%Y")
            for file in allFiles if '~$' not in file
            # if str(now.month) not in file  # code để test trong lúc dev vì file hiện tại đang được chỉnh hàng ngày
        ]
        maxTimes = max(allTimesInFile).strftime("%d.%m.%Y")
        fileName = list(filter(lambda x: maxTimes in x, allFiles))
        if len(fileName) > 1:
            raise InputDataError('Have more than 1 file to read or File is being opened!')
        filePath = join(self.__path, fileName[0])
        self.__result = pd.read_excel(
            filePath,
            sheet_name='Sheet1',
            usecols='B,H'
        )
        self.__result.rename(
            {'Mã CK': 'Stock', 'Room chung': 'IntranetGeneralRoom'},
            axis=1,
            inplace=True
        )
        return self.__result

    @property
    def HighRisk(self) -> pd.DataFrame:

        now = dt.datetime.now()
        self.__path = join(fr"{PATH.pattern['PATH'].get('HighRisk')}", str(now.year))

        # tìm folder mới nhất
        allFolders = listdir(self.__path)
        allTimesInFolders = [
            self.convertStringToDateTime(re.search(r'\d{1,2}\.\d{4}', file).group(), "%m.%Y")
            for file in allFolders
            # if str(now.month) not in file  # code để test trong lúc dev vì file hiện tại đang được chỉnh hàng ngày
        ]
        maxTimes = max(allTimesInFolders).strftime("%m.%Y")
        monthPath = list(filter(lambda x: maxTimes in x, allFolders))
        if len(monthPath) > 1:
            raise InputDataError('Have more than 1 folder to join or File is being opened!')
        filePath = join(self.__path, monthPath[0])

        # tìm file mới nhất trong folder
        allFilesHighRisk = [
            file
            for file in listdir(filePath)
            if re.search('summaryhighrisk', re.sub(r'\s+', '', file.lower())) and '~$' not in file
        ]
        allTimesInFilesHighRisk = [
            self.convertStringToDateTime(re.search(r'\d{8}', file).group(), "%d%m%Y")
            for file in allFilesHighRisk
        ]
        maxTimes = max(allTimesInFilesHighRisk).strftime("%d%m%Y")
        fileName = list(filter(lambda x: maxTimes in x, allFilesHighRisk))
        if len(fileName) > 1:
            raise InputDataError('Have more than 1 file to read or File is being opened!')
        finalPath = join(filePath, fileName[0])
        # read file excel
        self.__result = pd.read_excel(
            finalPath,
            sheet_name='Liquidity Deal Report',
            usecols='F,J',
            skiprows=1
        )
        # xóa các khoảng trắng ở đầu và cuối trong header
        self.__result.columns = self.__result.columns.str.strip()
        # cả 2 cột đều có dòng bị nan
        self.__result.dropna(how='all', inplace=True)
        # groupby theo mã (sum) - nếu 1 mã xuất hiện nhiều lần
        self.__result = self.__result.groupby('Stock')['Approved Quantity'].sum().reset_index()
        # đổi tên cột
        self.__result.rename(
            {'Approved Quantity': 'HighRiskApprovedQuantity'},
            axis=1,
            inplace=True
        )

        return self.__result

    @property
    def blackList(self) -> pd.DataFrame:
        self.__path = join(fr"{PATH.pattern['PATH'].get('BlackList')}", "Black List_Review MP Report.xlsx")
        self.__result = pd.read_excel(
            self.__path,
            sheet_name='Sheet1',
            usecols='A'
        )
        return self.__result
