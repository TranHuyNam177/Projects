import pandas as pd
import datetime as dt

from request import connect_DWH_AppData
from datawarehouse import BATCHINSERT, UPDATE

def get_status(
        in_file: str,
        target_file: str,
        target_db: str
):

    if in_file == 'left_only':
        return 'I'
    if target_file != target_db:
        return 'U'
    else:
        return 'N'

def run(
        run_date: dt.datetime = dt.datetime.now()
):
    target_file_path = rf"""D:\DataAnalytics\ETL\Data\Format chỉ tiêu {run_date.year}.xlsx"""

    mappingTable = {
        'CKCS': 'BMD.FlexTarget',
        'CKPS': 'BMD.FDSTarget'
    }

    for sheet in ['CKCS', 'CKPS']:
        target_table = pd.read_excel(
            target_file_path,
            usecols='A:C,E',
            sheet_name=sheet,
            dtype={
                'Target': str,
                'BranchID': str
            }
        )
        target_table.fillna('0', inplace=True)
        target_table['IntegrationID'] = target_table.apply(
            lambda x: x['BranchID'] + '~' + str(x['Year']) + '~' + x['Measure'],
            axis=1
        )
        # tìm tên bảng trong DB
        table_name = mappingTable.get(sheet)
        # data trong DB
        data_DB = pd.read_sql(
            sql=f"""
                SELECT [IntegrationID], [Target] [TargetDB]
                FROM [{table_name}] WHERE [Year] = {run_date.year}
            """,
            con=connect_DWH_AppData
        )
        final_table = pd.merge(
            left=target_table,
            right=data_DB,
            how='outer',
            on=['IntegrationID'],
            indicator='File:DB'
        )
        # tạo 2 cột mask
        final_table['InFile'] = final_table['File:DB']

        # xóa cột indicator
        final_table = final_table.drop('File:DB', axis=1)

        # tìm Status
        final_table['RecordStatus'] = final_table.apply(
            lambda x: get_status(x['InFile'], x['Target'], x['TargetDB'])
            , axis=1
        )
        # check table empty
        check_table = final_table.loc[
            (final_table['RecordStatus'] == 'U') |
            (final_table['RecordStatus'] == 'I')
        ]
        if check_table.empty:
            continue

        update_table = final_table.loc[
            final_table['RecordStatus'] == 'U'
        ].copy()

        insert_table = final_table.loc[
            final_table['RecordStatus'] == 'I'
        ].copy()

        for idx in update_table.index:
            UPDATE(
                conn=connect_DWH_AppData,
                table=table_name,
                where=f"[IntegrationID] = '{update_table.loc[idx, 'IntegrationID']}'",
                Target=update_table.loc[idx, 'Target']
            )

        insert_table = insert_table.drop(['TargetDB', 'InFile', 'RecordStatus'], axis=1)
        if not insert_table.empty:
            BATCHINSERT(
                connect_DWH_AppData,
                table_name,
                insert_table
            )