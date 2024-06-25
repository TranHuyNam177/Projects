import pandas as pd
from datawarehouse import BATCHINSERT, DROPDUPLICATES
from news_analysis import get_news
from request import connect_DWH_ThiTruong

def update(hours):

    """
    This function updates data to table [DWH-ThiTruong].[dbo].[TinChungKhoan]

    :param hours: number of hours to read news historically (đọc lùi bao nhiêu giờ)
    """

    Table1 = get_news.cafef(hours).run()
    print('Done cafef')
    # Table2 = get_news.ndh(hours).run()
    # print('Done ndh')
    Table3 = get_news.vietstock(hours).run()
    print('Done vietstock')
    # Table4 = get_news.tinnhanhchungkhoan(hours).run()
    # print('Done tinhanhchungkhoan')

    # newsTable = pd.concat([Table1,Table2,Table3,Table4])
    # newsTable = pd.concat([Table1,Table3,Table4])
    newsTable = pd.concat([Table1,Table3])

    BATCHINSERT(connect_DWH_ThiTruong,'TinChungKhoan',newsTable)
    DROPDUPLICATES(connect_DWH_ThiTruong,'TinChungKhoan','URL') # xóa dòng có URL trùng nhau

