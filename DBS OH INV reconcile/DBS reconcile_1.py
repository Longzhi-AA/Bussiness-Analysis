import pandas as pd
import os, datetime

app_time = datetime.date.today()


def merge_csv(file_path, file_list, data_fileds):
    print('working on processing {}.......'.format(file_list[0]))
    df = pd.read_excel(file_path + '/' + file_list[0], skiprows=1)
    if 'Supplier ID' not in df.keys():
        df = pd.read_excel(file_path + '/' + file_list[0], skiprows=2)
    df = df[data_fileds]
    source_name = str(file_list[0]).split('_')[1].split(".")[0]
    df['WHS Name'] = source_name
    for file in file_list[1:]:
        print('working on processing {}.......'.format(file))
        try:
            df2 = pd.read_excel(file_path + '/' + file, skiprows=1)
            if 'Supplier ID' not in df2.keys():
                df2 = pd.read_excel(file_path + '/' + file, skiprows=2)
        except Exception as e:
            print('**Note**:{} is not emerged successfully! Error:{}'.format(file, e))
            continue
        df2 = df2[data_fileds]
        df2['WHS Name'] = str(file).split('_')[1].split(".")[0]
        df = pd.concat([df, df2], axis=0, ignore_index=True, sort=False)
    df['Append_date'] = app_time
    df['AWS Sku'] = df['AWS Sku'].str[0:10]
    # df.drop_duplicates()
    df.reset_index(drop=True)
    return df


def merge_receipt(receipt_path, receipt_list):
    print('starting process receipt details............')
    data_fileds = ['Supplier ID', 'AWS Sku',
                   'Sku Description', 'Supplier Sku', 'Qty\nExpected', 'Qty\nReceived',
                   'Date\nExpected', 'Date\nReceived']
    receipt_df = merge_csv(receipt_path, receipt_list, data_fileds)
    receipt_df.columns = ['Supplier ID', 'AWS Sku',
                          'Sku Description', 'Supplier Sku', 'Qty Expected', 'Qty Received',
                          'Date Expected', 'Date Received', 'WHS Name', 'Append_date']
    receipt_df['Date Received'] = pd.to_datetime(receipt_df['Date Received'], dayfirst=False)
    print('start excluding items which quantity is 0 or null')
    receipt_df['Qty Received'] = receipt_df['Qty Received'].fillna(0)
    receipt_df['Date Received'] = receipt_df['Date Received'].fillna('x')
    receipt_df.reset_index(drop=True)
    receipt_df.drop(receipt_df[receipt_df['Qty Received'] == 0].index, inplace=True)
    receipt_df.drop(receipt_df[receipt_df['Date Received'] == 'x'].index, inplace=True)
    print('starting excluding suppliers that are not 440 or 480')
    all_list = list(set(list(receipt_df['Supplier ID'])))
    exclude_list = []
    for i in all_list:
        if '440' not in i and '480' not in i:
            exclude_list.append(i)
    for m in exclude_list:
        receipt_df.drop(receipt_df[receipt_df['Supplier ID'] == m].index, inplace=True)
    # receipt_df.index.name = 'Index'
    receipt_df.reset_index(drop=True)
    try:
        receipt_df.to_csv(c_path + '/' + 'receipt_total.csv', index=False)  # Windows电脑 '/'可能需设置为 '\\', MAC电脑设置为'/'
        print('''created csv file
        file location:{}; file name:receipt_total.csv
        ***receipt details emerging completed!
        ---------------------------------------'''.format(c_path))
    except Exception as e:
        print('****Something wrong when appending data, error message:{}'.format(e))


def merge_onhand(oh_path, oh_list):
    print('starting process on hand inventory details............')
    data_fileds = ['Supplier ID', 'AWS Sku', 'Supplier Sku', 'Sku Description', 'Qty']
    oh_df = merge_csv(oh_path, oh_list, data_fileds)
    print('start excluding items which quantity is 0 or null')
    oh_df['Qty'] = oh_df['Qty'].fillna(0)
    oh_df.reset_index(drop=True)
    oh_df.drop(oh_df[oh_df['Qty'] == 0].index, inplace=True)
    print('starting excluding suppliers that are not 440 or 480')
    all_list = list(set(list(oh_df['Supplier ID'])))
    exclude_list = []
    for i in all_list:
        if '440' not in i and '480' not in i:
            exclude_list.append(i)
    for m in exclude_list:
        oh_df.drop(oh_df[oh_df['Supplier ID'] == m].index, inplace=True)
    oh_df.reset_index(drop=True)
    try:
        oh_df.to_csv(c_path + '/' + 'oh_total.csv', index=False)  # Windows电脑 '/'需设置为 '\\', MAC电脑设置为'/'
        print('''created csv file
        file location:{}; file name:oh_total.csv
        ***onhand inventory emerging completed!
        ---------------------------------------'''.format(c_path))
    except Exception as e:
        print('****Something wrong when appending data, error message:{}'.format(e))


def merge_shipdetails(shipped_path, shipped_list):
    print('starting process ship details............')
    data_fileds = ['Supplier ID', 'AWS Sku', 'External\nOrder #',
                   'Sku Description', 'Supplier Sku', 'Quantity', 'Order\nAdd Date', 'Tracking Number'
                   ]
    data_fileds2 = ['Supplier ID', 'AWS Sku', 'External\nOrder #',
                    'Sku Description', 'Supplier Sku', 'Quantity', 'Order\nAdd Date'
                    ]
    print('working on processing {}.......'.format(shipped_list[0]))
    shipped_df = pd.read_excel(shipped_path + '/' + shipped_list[0], skiprows=1)
    if 'Supplier ID' not in shipped_df.keys():
        shipped_df = pd.read_excel(shipped_path + '/' + shipped_list[0], skiprows=2)
    try:
        shipped_df = shipped_df[data_fileds]
    except KeyError:
        shipped_df = shipped_df[data_fileds2]
    source_name = str(shipped_list[0]).split('_')[1].split(".")[0]
    shipped_df['WHS Name'] = source_name

    for file in shipped_list[1:]:
        print('working on processing {}.......'.format(file))
        try:
            df2 = pd.read_excel(shipped_path + '/' + file, skiprows=1)
            if 'Supplier ID' not in df2.keys():
                df2 = pd.read_excel(shipped_path + '/' + file, skiprows=2)
        except Exception as e:
            print('**Note**:{} is not emerged successfully! Error:{}'.format(file, e))
            continue
        try:
            df2 = df2[data_fileds]
        except KeyError:
            df2 = df2[data_fileds2]
        df2['WHS Name'] = str(file).split('_')[1].split(".")[0]
        shipped_df = pd.concat([shipped_df, df2], axis=0, ignore_index=True, sort=False)
    shipped_df.columns = ['Supplier ID', 'AWS Sku', 'External Order#',
                          'Sku Description', 'Supplier Sku', 'Quantity', 'Order Add Date', 'Tracking Number',
                          'WHS Name']
    shipped_df['Order Add Date'] = pd.to_datetime(shipped_df['Order Add Date'])
    shipped_df['AWS Sku'] = shipped_df['AWS Sku'].str[0:10]
    shipped_df['Append_date'] = app_time
    shipped_df.drop_duplicates()
    shipped_df.reset_index(drop=True)
    print('start excluding items which quantity is 0 or null')
    shipped_df['Quantity'] = shipped_df['Quantity'].fillna(0)
    shipped_df.drop(shipped_df[shipped_df['Quantity'] == 0].index, inplace=True)
    print('starting excluding suppliers that are not 440 or 480')
    all_list = list(set(list(shipped_df['Supplier ID'])))
    exclude_list = []
    for i in all_list:
        if '440' not in i and '480' not in i:
            exclude_list.append(i)
    for m in exclude_list:
        shipped_df.drop(shipped_df[shipped_df['Supplier ID'] == m].index, inplace=True)
    shipped_df.reset_index(drop=True)
    print('starting write to csv file')
    try:
        shipped_df.to_csv(c_path + '/' + 'ship_total.csv', index=False)  # Windows电脑 '/'需设置为 '\\', MAC电脑设置为'/'
        print('''created csv file
        file location:{}; file name:ship_total.csv
        ***shipped details emerging completed!
        ---------------------------------------'''.format(c_path))
    except Exception as e:
        print('****Something wrong when appending data, error message:{}'.format(e))


if __name__ == '__main__':
    date = input('please input date(eg: 6/01/2020, input 6.01 (month/day):')
    receipt_path = 'E:\BI\DBS reconc\DBS data\AWS {}\inbound details'.format(date)
    oh_path = 'E:\BI\DBS reconc\DBS data\AWS {}\onhand inventory'.format(date)
    shipped_path = 'E:\BI\DBS reconc\DBS data\AWS {}\outbound details'.format(date)

    c_path = os.path.dirname(receipt_path)
    receipt_list = os.listdir(receipt_path)
    shipped_list = os.listdir(shipped_path)
    oh_list = os.listdir(oh_path)

    merge_receipt(receipt_path, receipt_list)
    merge_onhand(oh_path, oh_list)
    merge_shipdetails(shipped_path, shipped_list)
