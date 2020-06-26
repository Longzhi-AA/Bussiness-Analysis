import pandas as pd
import datetime
from BI_team.append_df_to_excel import append_df_to_excel

location_df = pd.read_excel('E:/BI/DBS reconc/location_mapping.xlsx',
                            dtype={'Location#': 'str', 'Location': 'str'}
                            )
warehouse_df = pd.read_excel('E:/BI/DBS reconc/warehouse_mapping.xlsx')


def get_dfs(hyve_path, dbs_path):
    print('working on proccess hyve dataframe......')
    hyve_df = pd.read_csv(hyve_path,
                          usecols=['inv_region_name','inv_loc_no','Program','mfg_part_no','customer_part_no','unit_cost','quantity'],
                          dtype={'inv_region_name': 'str', 'inv_loc_no': 'str', 'quantity': 'int'},
                          )
    hyve_df['inv_region_name'] = hyve_df['inv_region_name'].str.replace('HYUS', 'US')
    hyve_df['Cluster'] = 'Hyve'
    hyve_df['Program'] = hyve_df['Program'].str.replace('Woody OMD', 'OMD')
    hyve_df['Program'] = hyve_df['Program'].str.replace('Woody Sparetacus', 'Sparetacus')
    hyve_df = pd.merge(hyve_df, location_df,
                       left_on='inv_loc_no',
                       right_on='Location#',
                       how='left')

    # 创建干净的hyve df
    pure_hyve_df = pd.DataFrame()
    pure_hyve_df['MFG Part#'] = hyve_df['mfg_part_no']
    pure_hyve_df['Region'] = hyve_df['inv_region_name']
    pure_hyve_df['OH Qty'] = hyve_df['quantity']
    pure_hyve_df['Cluster'] = hyve_df['Cluster']
    pure_hyve_df['Program'] = hyve_df['Program']
    pure_hyve_df['Location'] = hyve_df['Location']
    pure_hyve_df['IPN'] = hyve_df['customer_part_no']
    pure_hyve_df['SysCost($)'] = hyve_df['unit_cost']
    pure_hyve_df['Last Refresh Date'] = datetime.date.today()
    print('*******Hyve dataframe is ready........')

    # 处理dbs
    print('working on proccess dbs dataframe......')
    dbs_df = pd.read_csv(dbs_path,
                         dtype={'Qty': 'int'},
                         usecols=['Supplier ID', 'AWS Sku', 'Supplier Sku', 'Sku Description', 'Qty'])
    hyve_df_temp = pd.DataFrame()
    hyve_df_temp['Supplier Sku'] = hyve_df['mfg_part_no']
    hyve_df_temp['unit_cost'] = hyve_df['unit_cost']
    hyve_df_temp['qty'] = hyve_df['quantity']
    hyve_df_temp['sumtotal'] = hyve_df_temp['unit_cost'] * hyve_df_temp['qty']
    temp_df = hyve_df_temp.groupby('Supplier Sku', as_index=False).agg({
        'qty': 'sum',
        'sumtotal': 'sum'
    })
    temp_df['SysCost($)'] = temp_df['sumtotal'] / temp_df['qty']
    dbs_df = pd.merge(dbs_df, warehouse_df, on='Supplier ID', how='left')
    dbs_df = pd.merge(dbs_df, temp_df, on='Supplier Sku', how='left', validate='m:1')

    # 创建纯净的dbs df
    pure_dbs_df = pd.DataFrame()
    pure_dbs_df['MFG Part#'] = dbs_df['Supplier Sku']
    pure_dbs_df['Region'] = dbs_df['Region']
    pure_dbs_df['OH Qty'] = dbs_df['Qty']
    pure_dbs_df['Cluster'] = 'DBS'
    pure_dbs_df['Program'] = dbs_df['Program']
    pure_dbs_df['Location'] = dbs_df['Location']
    pure_dbs_df['IPN'] = dbs_df['AWS Sku']
    pure_dbs_df['SysCost($)'] = dbs_df['SysCost($)']
    pure_dbs_df['Last Refresh Date'] = datetime.date.today()
    print('*******DBS dataframe is ready........')

    return pure_hyve_df, pure_dbs_df


def append_to_dataset(dataset_path):
    hyve_df, dbs_df = get_dfs(hyve_path, dbs_path)
    sum_df = pd.concat([hyve_df, dbs_df], axis=0, ignore_index=True, sort=False)
    total_oh_df = pd.read_excel(dataset_path, sheet_name='Total_OH_INV')
    rows = total_oh_df['MFG Part#'].count()
    print('Appending data to dataset...')
    try:
        append_df_to_excel(dataset_path, sum_df,
                           sheet_name='Total_OH_INV',
                           index=False,
                           startrow=rows + 1)
        print('''------------------------------
        *******Successfully append data to excel!********''')
    except Exception as e:
        print('''--------------------------------
        *******Something wrong when appending data, error message:{}'''.format(e))


if __name__ == '__main__':
    date = input('please input date(eg: 6/01/2020, input 6.01 (month/day):')
    hyve_path = 'E:/BI/DBS reconc/HYVE data/data_{}.2020.csv'.format(date)
    dbs_path = 'E:/BI/DBS reconc/DBS data/AWS {}/oh_total.csv'.format(date)
    dataset_path = 'E:/BI/DBS reconc/dataset_inventory_gap_analysis.xlsx'

    append_to_dataset(dataset_path)

