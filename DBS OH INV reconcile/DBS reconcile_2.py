import pandas as pd
import datetime

def get_loc_ware_df(location_path, warehouse_path):
    print('Loading locations and warehouse........')
    location_df = pd.read_excel(location_path,
                                dtype={'Location#': 'str', 'Location': 'str'}
                                )
    warehouse_df = pd.read_excel(warehouse_path)
    return location_df, warehouse_df

def append_to_dataset(hyve_path, dbs_path, dataset_path):
    location_df, warehouse_df = get_loc_ware_df(location_path, warehouse_path)
    print('Loading hyve data......')
    hyve_df = pd.read_csv(hyve_path,
                          usecols=['inv_region_name','inv_loc_no','Program','mfg_part_no','part_no','customer_part_no','unit_cost','quantity'],
                          dtype={'inv_region_name': 'str', 'inv_loc_no': 'str', 'quantity': 'int'},
                          )
    print('Loading the original dataset......')
    dataset_df= pd.read_excel(dataset_path, sheet_name='Total_OH_INV',
                              dtype={'OH Qty':'int', 'IPN':'str', 'Last Refresh Date':'datetime64[ns]'})
    pure_dataset_df = dataset_df[['MFG Part#','HYVE Part#']]
    part_df = pd.DataFrame()
    part_df['MFG Part#'] = hyve_df['mfg_part_no']
    part_df['HYVE Part#'] = hyve_df['part_no']
    part_df = pd.concat([pure_dataset_df,part_df], axis=0, ignore_index=True, sort=False)
    part_df = part_df.dropna()
    part_df = part_df.reset_index()
    part_dic = {}
    print('create mapping list bettween MFG and HYVE Part#......')
    for i in range(part_df['MFG Part#'].count()):
        if part_df['MFG Part#'][i] not in part_dic.keys():
            part_dic[part_df['MFG Part#'][i]] = [part_df['HYVE Part#'][i]]
        elif part_df['MFG Part#'][i] in part_dic.keys() and part_df['HYVE Part#'][i] in part_dic[part_df['MFG Part#'][i]]:
            continue
        else:
            part_dic[part_df['MFG Part#'][i]].append(part_df['HYVE Part#'][i])
    l1 = []
    l2 = []
    for k,v in part_dic.items():
        l1.append(k)
        l2.append(str(v).replace("[", '').replace(']','').replace("'",''))
    part_mapping = pd.DataFrame({'MFG Part#':l1, 'HYVE Part#':l2})

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
    pure_hyve_df['HYVE Part#'] = None
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
    pure_dbs_df['HYVE Part#'] = None
    pure_dbs_df['Region'] = dbs_df['Region']
    pure_dbs_df['OH Qty'] = dbs_df['Qty']
    pure_dbs_df['Cluster'] = 'DBS'
    pure_dbs_df['Program'] = dbs_df['Program']
    pure_dbs_df['Location'] = dbs_df['Location']
    pure_dbs_df['IPN'] = dbs_df['AWS Sku']
    pure_dbs_df['SysCost($)'] = dbs_df['SysCost($)']
    pure_dbs_df['Last Refresh Date'] = datetime.date.today()
    print('*******DBS dataframe is ready........')
    print('working on merge all data to one dataframe.......')
    sum_df = pd.concat([pure_hyve_df, pure_dbs_df], axis=0, ignore_index=True, sort=False)
    total_sum_df = pd.concat([dataset_df,sum_df], axis=0, ignore_index=True, sort=False)
    # rows = dataset_df['MFG Part#'].count()
    # print('Appending data to dataset...')
    # try:
    #     append_df_to_excel(dataset_path, sum_df,
    #                        sheet_name='Total_OH_INV',
    #                        index=False,
    #                        startrow=rows + 1)
    #     print('''------------------------------
    #     *******Successfully append data to excel!********''')
    # except Exception as e:
    #     print('''--------------------------------
    #     *******Something wrong when appending data, error message:{}'''.format(e))
    total_sum_df = pd.merge(total_sum_df, part_mapping, how='left', on='MFG Part#', validate='m:1')
    pure_sum_df = pd.DataFrame()
    pure_sum_df['MFG Part#'] = total_sum_df['MFG Part#']
    pure_sum_df['HYVE Part#'] = total_sum_df['HYVE Part#_y']
    pure_sum_df['Region'] = total_sum_df['Region']
    pure_sum_df['OH Qty'] = total_sum_df['OH Qty']
    pure_sum_df['Cluster'] = total_sum_df['Cluster']
    pure_sum_df['Program'] = total_sum_df['Program']
    pure_sum_df['Location'] = total_sum_df['Location']
    pure_sum_df['IPN'] = total_sum_df['IPN']
    pure_sum_df['SysCost($)'] = total_sum_df['SysCost($)']
    pure_sum_df['Last Refresh Date'] = total_sum_df['Last Refresh Date']
    print('Exporting data to excel.......')
    pure_sum_df.to_excel(dataset_path, index=None,sheet_name='Total_OH_INV')
    dataset_df.to_excel(dataset_path.split('.')[0]+'_copy of last refresh day.xlsx', index=None,sheet_name='Total_OH_INV') #this is a copy for last version, keep this in case any errors in new version
    print('''------------------------------
            *******Successfully append data to excel!********''')


if __name__ == '__main__':
    location_path = 'E:/BI/DBS reconc/location_mapping.xlsx'
    warehouse_path = 'E:/BI/DBS reconc/warehouse_mapping.xlsx'

    date = input('please input date(eg: 6/01/2020, input 6.01 (month/day):')
    hyve_path = 'E:/BI/DBS reconc/HYVE data/data_{}.2020.csv'.format(date)
    dbs_path = 'E:/BI/DBS reconc/DBS data/AWS {}/oh_total.csv'.format(date)
    dataset_path = 'E:/BI/DBS reconc/dataset_inventory_gap_analysis.xlsx'

    append_to_dataset(hyve_path, dbs_path, dataset_path)

