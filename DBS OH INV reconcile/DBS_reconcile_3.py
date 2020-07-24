import pandas as pd
import os, datetime

app_time = datetime.date.today()


def merge_csv(file_path, file_list, data_fileds, data_fileds2):
    print('working on processing {}.......'.format(file_list[0]))
    df = pd.read_excel(file_path + '/' + file_list[0], skiprows=1)
    if 'Supplier ID' not in df.keys():
        df = pd.read_excel(file_path + '/' + file_list[0], skiprows=2)
    try:
        df = df[data_fileds]
    except KeyError:
        df = df[data_fileds2]
    source_name = str(file_list[0]).split('_')[1].split(".")[0]
    df['Location Name'] = source_name

    for file in file_list[1:]:
        print('working on processing {}.......'.format(file))
        try:
            df2 = pd.read_excel(file_path + '/' + file, skiprows=1)
            if 'Supplier ID' not in df2.keys():
                df2 = pd.read_excel(file_path + '/' + file, skiprows=2)
        except Exception as e:
            print('**Note**:{} is not emerged successfully! Error:{}'.format(file, e))
            continue
        try:
            df2 = df2[data_fileds]
        except KeyError:
            df2 = df2[data_fileds2]
        df2['Location Name'] = str(file).split('_')[1].split(".")[0]
        df = pd.concat([df, df2], axis=0, ignore_index=True, sort=False)
    df['Append_date'] = app_time
    df['AWS Sku'] = df['AWS Sku'].str[0:10]
    # df.drop_duplicates()
    df.reset_index(drop=True)
    return df


def merge_receipt(receipt_path, receipt_list):
    print('starting process receipt details............')
    data_fileds = ['Supplier','External\nASN #','WMS\nReceipt #','Mode of\nTransport','Carrier\nCode','Waybill #','Type','ASN Status',
              'Date\nExpected','Date\nReceived','External\nLine #','Supplier ID','Carton ID','Pallet ID','AWS Sku','Supplier Sku','Sku Description',
              'Hot\nPart','Hot Add Date','UOM','Qty\nExpected','Qty\nReceived','Qty Delta']
    data_fileds2 = ['Supplier', 'External\nASN #', 'WMS\nReceipt #', 'Mode of\nTransport', 'Carrier\nCode', 'Waybill #','Type', 'ASN Status',
                   'Date\nExpected', 'Date\nReceived', 'External\nLine #', 'Supplier ID', 'Carton ID', 'Pallet ID','AWS Sku', 'Supplier Sku', 'Sku Description',
                   'Hot\nPart', 'Hot Add Date', 'UOM', 'Qty\nExpected', 'Qty\nReceived', 'Qty Delta']
    # receipt_df = merge_csv(receipt_path, receipt_list,data_fileds, data_fileds2)

    print('working on processing {}.......'.format(receipt_list[0]))
    receipt_df = pd.read_excel(receipt_path + '/' + receipt_list[0], skiprows=1,
                               dtype={'External\nASN #':'str','WMS\nReceipt #':'str', 'Carton ID':'str','Pallet ID':'str'})
    if 'Supplier ID' not in receipt_df.keys():
        receipt_df = pd.read_excel(receipt_path + '/' + receipt_list[0], skiprows=2,
                                   dtype={'External\nASN #':'str','WMS\nReceipt #':'str', 'Carton ID':'str','Pallet ID':'str'})
    try:
        receipt_df = receipt_df[data_fileds]
    except KeyError:
        receipt_df = receipt_df[data_fileds2]
    source_name = str(receipt_list[0]).split('_')[1].split(".")[0]
    receipt_df['Location Name'] = source_name

    for file in receipt_list[1:]:
        print('working on processing {}.......'.format(file))
        try:
            df2 = pd.read_excel(receipt_path + '/' + file, skiprows=1,
                                dtype={'External\nASN #':'str','WMS\nReceipt #':'str', 'Carton ID':'str','Pallet ID':'str'})
            if 'Supplier ID' not in df2.keys():
                df2 = pd.read_excel(receipt_path + '/' + file, skiprows=2,
                                    dtype={'External\nASN #':'str','WMS\nReceipt #':'str', 'Carton ID':'str','Pallet ID':'str'})
        except Exception as e:
            print('**Note**:{} is not emerged successfully! Error:{}'.format(file, e))
            continue
        try:
            df2 = df2[data_fileds]
        except KeyError as e:
            try:
                df2 = df2[data_fileds2]
            except Exception as e:
                print('{} is not proceeded due to error: {}'.format(file,e))
        df2['Location Name'] = str(file).split('_')[1].split(".")[0]
        receipt_df = pd.concat([receipt_df, df2], axis=0, ignore_index=True, sort=False)
    receipt_df['Append_date'] = app_time
    receipt_df['AWS Sku'] = receipt_df['AWS Sku'].str[0:10]
    # df.drop_duplicates()
    receipt_df.reset_index(drop=True)

    receipt_df.columns = ['Supplier','External ASN #','WMS Receipt #','Mode of Transport','Carrier Code','Waybill #','Type','ASN Status',
              'Date Expected','Date Received','External Line #','Supplier ID','Carton ID','Pallet ID','AWS Sku','Supplier Sku','Sku Description',
              'Hot Part','Hot Add Date','UOM','Qty Expected','Qty Received','Qty Delta', 'Location Name', 'Append_date']
    receipt_df['Date Received'] = pd.to_datetime(receipt_df['Date Received'], infer_datetime_format=True)
    receipt_df['WMS Receipt #'] = receipt_df['WMS Receipt #'].astype(str)
    receipt_df['External ASN #'] = receipt_df['External Line #'].astype(str)
    receipt_df['Date Expected'] = pd.to_datetime(receipt_df['Date Expected'], infer_datetime_format=True)
    print('start excluding items which quantity is 0 or null')
    receipt_df['Qty Received'] = receipt_df['Qty Received'].fillna(0)
    # receipt_df['Date Received'] = receipt_df['Date Received'].fillna('x')
    receipt_df.reset_index(drop=True)
    receipt_df.drop(receipt_df[receipt_df['Qty Received'] == 0].index, inplace=True)
    receipt_df.reset_index(drop=True)
    try:
        os.mkdir(c_path + './phase2')
    except FileExistsError:
        pass
    try:
        receipt_df.to_excel(c_path + '/phase2'+ '/' + 'inbound.xlsx', index=False)  # Windows电脑 '/'可能需设置为 '\\', MAC电脑设置为'/'
        print('''created xls file
        file location:{}; file name:inbound.xlsx
        ***receipt details emerging completed!
        ---------------------------------------'''.format(c_path))
    except Exception as e:
        print('****Something wrong when appending data, error message:{}'.format(e))


def merge_onhand(oh_path, oh_list):
    print('starting process on hand inventory details............')
    data_fileds = ['Supplier ID', 'Supplier','AWS Sku', 'Supplier Sku','Sku Description', 'UOM','Location','Carton ID','Pallet ID','Qty',
              'Qty\nAllocated','Qty\nPicked','Qty\nOn Hold','Qty\nAvailable','Batch','Receipt','Date  Received','ASN Reference',
              'Reservation Status','COO','Reservation\nAdd Time','Resevation Order','WMS\nOrder #','External\nOrder #','Pull Request\nDate']
    data_fileds2 = ['Supplier ID', 'Supplier', 'AWS Sku', 'Supplier Sku', 'Sku Description', 'UOM', 'Location','Carton ID', 'Pallet ID', 'Qty',
                   'Qty\nAllocated', 'Qty\nPicked', 'Qty\nOn Hold', 'Qty\nAvailable', 'Receipt','Date  Received', 'ASN Reference',
                   'Reservation Status', 'COO', 'Reservation\nAdd Time', 'Resevation Order', 'WMS\nOrder #','External\nOrder #', 'Pull Request\nDate']

    print('working on processing {}.......'.format(oh_list[0]))
    oh_df = pd.read_excel(oh_path + '/' + oh_list[0], skiprows=1,
                          dtype={'Carton ID':'str', 'Pallet ID':'str', 'Receipt':'str'})
    if 'Supplier ID' not in oh_df.keys():
        oh_df = pd.read_excel(oh_path + '/' + oh_list[0], skiprows=2,
                              dtype={'Carton ID':'str', 'Pallet ID':'str', 'Receipt':'str'})
    if 'Batch' not in oh_df.keys():
        oh_df = oh_df[data_fileds2]
    else:
        oh_df = oh_df[data_fileds]
    source_name = str(oh_list[0]).split('_')[1].split(".")[0]
    oh_df['Location Name'] = source_name

    for file in oh_list[1:]:
        print('working on processing {}.......'.format(file))
        try:
            df2 = pd.read_excel(oh_path + '/' + file, skiprows=1,
                                dtype={'Carton ID':'str', 'Pallet ID':'str', 'Receipt':'str'})
            if 'Supplier ID' not in df2.keys():
                df2 = pd.read_excel(oh_path + '/' + file, skiprows=2,
                                    dtype={'Carton ID':'str', 'Pallet ID':'str', 'Receipt':'str'})
        except Exception as e:
            print('**Note**:{} is not emerged successfully! Error:{}'.format(file, e))
            continue
        if 'Batch' not in df2.keys():
            df2 = df2[data_fileds2]
        else:
            df2 = df2[data_fileds]
        df2['Location Name'] = str(file).split('_')[1].split(".")[0]
        oh_df = pd.concat([oh_df, df2], axis=0, ignore_index=True, sort=False)
    oh_df['Append_date'] = app_time
    oh_df['AWS Sku'] = oh_df['AWS Sku'].str[0:10]
    # df.drop_duplicates()
    oh_df.reset_index(drop=True)

    oh_df.columns = ['Supplier ID', 'Supplier','AWS Sku', 'Supplier Sku','Sku Description', 'UOM','Location','Carton ID','Pallet ID','Qty',
              'Qty Allocated','Qty Picked','Qty On Hold','Qty Available','Batch','Receipt','Date  Received','ASN Reference',
              'Reservation Status','COO','Reservation Add Time','Resevation Order','WMS Order #','External Order#','Pull Request Date','Location Name', 'Append_date']
    print('start excluding items which quantity is 0 or null')
    oh_df['Qty'] = oh_df['Qty'].fillna(0)
    oh_df.reset_index(drop=True)
    oh_df.drop(oh_df[oh_df['Qty'] == 0].index, inplace=True)
    oh_df.reset_index(drop=True)

    try:
        oh_df.to_excel(c_path + '/phase2' + '/' + 'oh_inventory.xlsx', index=False)  # Windows电脑 '/'需设置为 '\\', MAC电脑设置为'/'
        print('''created xlsx file
        file location:{}; file name:oh_inventory.xlsx
        ***onhand inventory emerging completed!
        ---------------------------------------'''.format(c_path))
    except Exception as e:
        print('****Something wrong when appending data, error message:{}'.format(e))


def merge_shipdetails(shipped_path, shipped_list):
    print('starting process ship details............working on processing {}.......'.format(shipped_list[0]))
    shipped_df = pd.read_excel(shipped_path + '/' + shipped_list[0], skiprows=1)
    if 'Supplier ID' not in shipped_df.keys():
        shipped_df = pd.read_excel(shipped_path + '/' + shipped_list[0], skiprows=2)
    source_name = str(shipped_list[0]).split('_')[1].split(".")[0]
    shipped_df['Location Name'] = source_name

    for file in shipped_list[1:]:
        print('working on processing {}.......'.format(file))
        try:
            df2 = pd.read_excel(shipped_path + '/' + file, skiprows=1)
            if 'Supplier ID' not in df2.keys():
                df2 = pd.read_excel(shipped_path + '/' + file, skiprows=2)
        except Exception as e:
            print('**Note**:{} is not emerged successfully! Error:{}'.format(file, e))
            continue
        df2['Location Name'] = str(file).split('_')[1].split(".")[0]
        shipped_df = pd.concat([shipped_df, df2], axis=0, ignore_index=True, sort=False)
    # print(shipped_df.count())
    ex_datafeilds = ['Stock Status at Order Drop', 'Last Event Description', 'Event Location', 'Event Date', 'Delivered\nDate','Strategic Commodity Flag', 'Permit\nNumber','BUYERPO','NOTES1','NOTES2']
    shipped_df = shipped_df.drop(columns=ex_datafeilds)
    shipped_df.columns = ['WMS Order#','External Order#','External Line#', 'Order Add Date','Actual Ship Date','Priority','Order Type','Order Status','ODM ID','Supplier ID',
              'AWS Sku','Supplier Sku','Sku Description','Quantity','Drop ID','Carton ID','Pallet ID','Batch','Move From Location','ASN Reference','Packing List #',
              'Reservation Add Time','Resevation Order','Reservation to Release Aging','SPARESREQ','Buyer PO','Quantity Ordered','COO','Tracking Number ',
              'Quantity Allocated','Need By Date','Location Name']
    shipped_df['Order Add Date'] = pd.to_datetime(shipped_df['Order Add Date'])
    shipped_df['AWS Sku'] = shipped_df['AWS Sku'].str[0:10]
    shipped_df['Append_date'] = app_time
    shipped_df.drop_duplicates()
    shipped_df.reset_index(drop=True)
    print('start excluding items which quantity is 0 or null')
    shipped_df['Quantity'] = shipped_df['Quantity'].fillna(0)
    shipped_df.drop(shipped_df[shipped_df['Quantity'] == 0].index, inplace=True)
    print('start writing to csv file')
    try:
        shipped_df.to_excel(c_path + '/phase2' + '/' + 'outbound.xlsx', index=False)  # Windows电脑 '/'需设置为 '\\', MAC电脑设置为'/'
        print('''created xlsx file
        file location:{}; file name:outbound.xlsx
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
