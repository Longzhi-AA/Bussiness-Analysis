import pandas as pd
import datetime


def normlize_ref(df):
    try:
        df['Ref'] = df['Ref'].str.lower()
        df['Ref'] = df['Ref'].str.title()
    except KeyError:
        pass
    try:
        df['Vend Name'] = df['Vend Name'].str.lower()
        df['Vend Name'] = df['Vend Name'].str.title()
    except KeyError:
        pass
    return df


def get_fiscalyear(months):
    cur_month = months[-1]
    if cur_month.split('-')[1] == '12':
        fy_months = [cur_month]
        return fy_months
    else:
        y_months = months[-12:]
        for i, m in enumerate(y_months):
            if m.split('-')[1] == '12':
                fy_months = y_months[i:]
                return fy_months


def get_month(cur_month):
    month_dic = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun', '07': 'Jul',
                 '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}
    month = cur_month.split('-')[1]
    month_name = month_dic[month]
    return month, month_name


def get_current_quarter(data, months):
    quart = {'Q1_2020': ['2019-12', '2020-01', '2020-02'], 'Q2_2020': ['2020-03', '2020-04', '2020-05'],
             'Q3_2020': ['2020-06', '2020-07', '2020-08'], 'Q4_2020': ['2020-09', '2020-10', '2020-11']}
    for Q in quart.keys():
        try:
            data[Q] = data[quart[Q][0]] + data[quart[Q][1]]
        except KeyError:
            data[Q] = data[quart[Q][0]]
        try:
            data[Q] = data[quart[Q][2]] + data[Q]
        except KeyError:
            pass
        if months[-1] in quart[Q]:
            return Q


def get_quarters(cur_quarter):
    if cur_quarter.split('_')[0] == 'Q1':
        last_quarter = 'Q4_' + str(int(cur_quarter.split('_')[1]) - 1)
        last_last_quarter = 'Q3_' + str(int(cur_quarter.split('_')[1]) - 1)
        last_quarter_lastyear = 'Q4_' + str(int(cur_quarter.split('_')[1]) - 2)
    elif cur_quarter.split('_')[0] == 'Q2':
        last_quarter = 'Q1_' + cur_quarter.split('_')[1]
        last_last_quarter = 'Q4_' + str(int(cur_quarter.split('_')[1]) - 1)
        last_quarter_lastyear = 'Q1_' + str(int(cur_quarter.split('_')[1]) - 1)
    elif cur_quarter.split('_')[0] == 'Q3':
        last_quarter = 'Q2_' + cur_quarter.split('_')[1]
        last_last_quarter = 'Q1_' + cur_quarter.split('_')[1]
        last_quarter_lastyear = 'Q2_' + str(int(cur_quarter.split('_')[1]) - 1)
    else:
        last_quarter = 'Q3_' + cur_quarter.split('_')[1]
        last_last_quarter = 'Q2_' + cur_quarter.split('_')[1]
        last_quarter_lastyear = 'Q3_' + str(int(cur_quarter.split('_')[1]) - 1)

    return last_quarter, last_last_quarter, last_quarter_lastyear


def get_progress(x, y, z, r):
    if x > 0 and y == 0 and z == 0:
        a = 'New'
    elif x > 0 and y == 0 and z > 0:
        a = 'Win-Back'
    elif x == 0 and y == 0 and z == 0:
        a = 'Oppty'
    elif x > 0 and y >= 0 and z >= 0 and r >= z:
        a = 'Existing'
    else:
        a = 'Slipped'
    return a

# step 1: refresh data_TAM
def process_data_TAM(data_path, pos_path, customer_path):
    cust_feilds = ['cust_no', 'cust_name', 'division', 'division_desc', 'cust_type', 'cust_type_descr',
                   'sales_manager_name', 'sales_terr', 'terr_name',
                   'restricted', 'discontinued', 'default_terms', 'mcust_no', 'mcust_name', 'credit_master_acct',
                   'Data Refresh Date']
    print('Loading customer dataset ......')
    cust_dataset_df = pd.read_excel(customer_path, skiprows=2,
                                    sheet_name='Sheet1',
                                    usecols=cust_feilds,
                                    dtype={'cust_no': 'str', 'mcust_no': 'str', 'credit_master_acct': 'str'}
                                    )
    print('Loading data TAM ......')
    tam_df = pd.read_excel(data_path,
                           sheet_name='data_TAM',
                           usecols=['Vend Name', 'Partner Name', 'Partner level', 'MCN#', 'Credit MCN', 'Grouping Name',
                                    'Revenue Requirements (Annual)'],
                           dtype={'MCN#': 'str', 'Credit MCN': 'str', 'Revenue Requirements (Annual)': 'float'})
    print('Working on refresh MCN ......')
    simplfied_cust_dataset_df = cust_dataset_df[['cust_no', 'mcust_no', 'credit_master_acct']].drop_duplicates()
    simplfied_tam_df = tam_df[['MCN#']].drop_duplicates()
    temp_df = pd.merge(simplfied_tam_df, simplfied_cust_dataset_df, left_on='MCN#', right_on='cust_no', how='left',
                       validate='1:1')

    tam_df = pd.merge(tam_df, temp_df, left_on='MCN#', right_on='MCN#', how='left', validate='m:1')
    tam_df = tam_df.drop(columns=['MCN#', 'Credit MCN', 'cust_no'])
    tam_df.columns = ['Vend Name', 'Partner Name', 'Partner level', 'Grouping Name', 'Revenue Requirements (Annual)',
                      'MCN#', 'Credit MCN']
    tam_df['Ref-MCN'] = tam_df['Vend Name'] + '-' + tam_df['MCN#']
    tam_df['Ref-CMCN'] = tam_df['Vend Name'] + '-' + tam_df['Credit MCN']
    tam_df['Ref-reseller name'] = tam_df['Vend Name'] + '-' + tam_df['Grouping Name']
    # date = datetime.date.today()
    tam_df.to_excel('data_TAM.xlsx', index=False, sheet_name='data_TAM')
    print('''---------------------------------
    *********Done! Successfuly proceeded data_TAM!*******''')

    return tam_df, cust_dataset_df


# step 2: refresh data_Sales
def process_data_sales(data_path, pos_path, customer_path):
    print('Loading data Sales ......')
    data_sales_df = pd.read_excel(data_path,
                                  sheet_name='data_Sales',
                                  dtype={'MCN#': 'str'})

    print('Loading POS data ......')
    pos_data_df = pd.read_excel(pos_path,
                                sheet_name='sheet1',
                                usecols=['order_no', 'order_type', 'order_line_no', 'Ship_Date', 'sku_no', 'part_no',
                                         'Ship_Qty', 'Extend_Net_Price', 'vend_no', 'master_cust_no'],
                                dtype={'order_no': 'str', 'order_type': 'str', 'sku_no': 'str', 'Ship_Qty': 'int',
                                       'vend_no': 'str', 'master_cust_no': 'str'})
    pos_data_df['Ship_Date'] = pd.to_datetime(pos_data_df['Ship_Date'], format='%d/%B/%Y')
    print('Loading vendor mapping list ......')
    vendors_df = pd.read_excel(data_path,
                               sheet_name='Vend Mapping list', skiprows=1,
                               usecols=['Vend', 'Vend#'],
                               dtype={'Vend#': 'str'})
    vnumber_mapping = pos_data_df['vend_no'].drop_duplicates()
    vnumber_mapping = pd.merge(vnumber_mapping, vendors_df, left_on='vend_no', right_on='Vend#', how='left',
                               validate='1:m').drop(columns='Vend#')
    pos_data_df = pd.merge(pos_data_df, vnumber_mapping, on='vend_no', how='left', validate='m:1')
    pos_data_df['Ref'] = pos_data_df['Vend'] + '-' + pos_data_df['master_cust_no']
    pos_data_df['Month'] = pd.DatetimeIndex(pos_data_df['Ship_Date']).month
    pos_data_df['Year'] = pd.DatetimeIndex(pos_data_df['Ship_Date']).year
    pos_data_df['Date'] = pd.to_datetime(
        pd.DataFrame({'year': pos_data_df['Year'], 'month': pos_data_df['Month'], 'day': 1}))
    pivot_pos = pos_data_df.pivot_table(index='Ref', columns=['Date'], values='Extend_Net_Price', aggfunc=sum)
    pivot_pos = pivot_pos.reset_index()
    pivot_pos['Vend Name'] = pivot_pos['Ref'].str.split('-', 1).str[0]
    pivot_pos['MCN#'] = pivot_pos['Ref'].str.split('-', 1).str[1]

    col_list = []
    for col in pivot_pos.columns:
        if '00:00:00' in str(col):
            new_col = str(col).split(' ')[0][:7]
            col_list.append(new_col)
        else:
            col_list.append(col)
    pivot_pos.columns = col_list
    print('Append data to data_Sales.......')
    data_sales_df = pd.merge(data_sales_df, pivot_pos, on=['Ref', 'Vend Name', 'MCN#'], how='outer')
    for i in data_sales_df.columns:
        if '_x' in str(i):
            data_sales_df = data_sales_df.drop(columns=i)
        elif '_y' in str(i):
            data_sales_df = data_sales_df.rename(columns={i: str(i).split('_')[0]})
    months = []
    for col in data_sales_df.columns:
        months.append(col)
    # date = datetime.date.today()
    data_sales_df.to_excel('data_sales.xlsx', index=False, sheet_name='data_Sales')
    print('''---------------------------------
    *********Done! Successfuly proceeded data_Sales!*******''')

    return data_sales_df, months[3:]


# step 3: refresh data table

def process_data(data_path, pos_path, customer_path):
    bd_project_df = pd.read_excel(data_path,
                                  sheet_name='BD project info',
                                  usecols=['Ref', 'BD Project#', 'Target project#', 'BD Rep', 'Has BD Coverage?'],
                                  dtype={'MCN#': 'str', 'BD Project#': 'str', 'Target project#': 'str'}
                                  )
    Target_reseller_df = pd.read_excel(data_path,
                                       sheet_name='Target reseller list',
                                       dtype={'MCN#': 'str'}
                                       )
    sales_division = pd.read_excel('E:/Projects\BI/CA report/07.08/sales division.xlsx',
                                   dtype={'Sales Division ID': 'str'})
    sales_group = pd.read_excel('E:/Projects/BI/CA report/07.08/sales group.xlsx',
                                dtype={'Sales Group ID': 'str'})
    Target_reseller_df['Ref'] = Target_reseller_df['Vend Name'] + '-' + Target_reseller_df['MCN#']
    Target_reseller_df = Target_reseller_df.drop(columns=['REF', 'Vend Name', 'MCN#'])
    tam_df, cust_dataset_df = process_data_TAM(data_path, pos_path, customer_path)
    data_sales_df, months = process_data_sales(data_path, pos_path, customer_path)
    print('start processing data..........')
    cust_df = pd.DataFrame()
    cust_df['MCN#'] = cust_dataset_df['mcust_no'].astype('str')
    cust_df['Reseller'] = cust_dataset_df['mcust_name']
    cust_df['Terr#'] = cust_dataset_df['sales_terr'].astype('str')
    cust_df['Sales Group'] = cust_dataset_df['cust_type_descr']
    cust_df['Sales Division'] = cust_dataset_df['division_desc']
    cust_df['count'] = cust_df.groupby(['MCN#', 'Terr#'])['Terr#'].transform('count')

    cust_temp = pd.pivot_table(cust_df, values=['count', 'Terr#'], index=['MCN#'],
                               aggfunc={'count': max, 'Terr#': lambda x: str(x)})
    cust_temp['Terr#'] = cust_temp['Terr#'].str.split('\\n', 1).str[0]
    cust_temp['Terr#'] = cust_temp['Terr#'].str.split(' ', 1).str[1]
    cust_temp['Terr#'] = cust_temp['Terr#'].str.lstrip()
    cust_temp = cust_temp.reset_index()
    cust_temp = cust_temp.drop(columns=['count'])
    cust_temp = cust_temp.drop_duplicates()
    cust_temp = pd.merge(cust_temp, cust_df, on=['MCN#', 'Terr#'], how='left', validate='1:m')
    cust_temp = cust_temp.drop_duplicates()
    cust_temp = cust_temp.drop(columns=['count'])

    partner_level = pd.DataFrame()
    partner_level['Ref'] = tam_df['Ref-MCN']
    partner_level['Partner level'] = tam_df['Partner level']
    partner_level['Partner level'] = partner_level['Partner level'].fillna('(blank)')
    partner_level = partner_level.drop_duplicates()  # Q: parnter level 为空可否去掉（blank)直接表示为空
    data1 = pd.DataFrame()
    data2 = pd.DataFrame()
    data1['Ref'] = tam_df['Ref-MCN']
    data1['Vend Name'] = tam_df['Vend Name']
    data1['MCN#'] = tam_df['MCN#']
    data2['Ref'] = data_sales_df['Ref']
    data2['Vend Name'] = data_sales_df['Vend Name']
    data2['MCN#'] = data_sales_df['MCN#']

    data = pd.concat([data1, data2], axis=0, ignore_index=True, sort=False)
    data = normlize_ref(data)
    data = data.drop_duplicates()
    data = pd.merge(data, normlize_ref(partner_level), on='Ref', how='left', validate='1:1')
    data = pd.merge(data, normlize_ref(cust_temp), on='MCN#', how='left', validate='m:1')
    data = pd.merge(data, normlize_ref(bd_project_df), on='Ref', how='left', validate='1:1')
    data = pd.merge(data, normlize_ref(Target_reseller_df), on='Ref', how='left', validate='1:1')
    data['Has BD Coverage?'] = data['Has BD Coverage?'].fillna('No')
    data['Has existed on Target reseller list?'] = data['Has existed on Target reseller list?'].fillna('No')
    simplfied_tam_df = pd.DataFrame()
    simplfied_tam_df['Ref'] = tam_df['Ref-MCN']
    simplfied_tam_df['Revenue Requirements (Annual)'] = tam_df['Revenue Requirements (Annual)']
    simplfied_sales_df = data_sales_df.drop(columns=['Vend Name', 'MCN#'])

    data = pd.merge(data, normlize_ref(simplfied_sales_df), on='Ref', how='left')
    data = pd.merge(data, normlize_ref(simplfied_tam_df), on='Ref', how='left')
    data['Revenue Requirements (Annual)'] = data['Revenue Requirements (Annual)'].fillna(0)
    for month in months:
        data[month] = data[month].fillna(0)
    data['Q1_2017'] = data['2016-12'] + data['2017-01'] + data['2017-02']
    data['Q2_2017'] = data['2017-03'] + data['2017-04'] + data['2017-05']
    data['Q3_2017'] = data['2017-06'] + data['2017-07'] + data['2017-08']
    data['Q4_2017'] = data['2017-09'] + data['2017-10'] + data['2017-11']

    data['Q1_2018'] = data['2017-12'] + data['2018-01'] + data['2018-02']
    data['Q2_2018'] = data['2018-03'] + data['2018-04'] + data['2018-05']
    data['Q3_2018'] = data['2018-06'] + data['2018-07'] + data['2018-08']
    data['Q4_2018'] = data['2018-09'] + data['2018-10'] + data['2018-11']

    data['Q1_2019'] = data['2018-12'] + data['2019-01'] + data['2019-02']
    data['Q2_2019'] = data['2019-03'] + data['2019-04'] + data['2019-05']
    data['Q3_2019'] = data['2019-06'] + data['2019-07'] + data['2019-08']
    data['Q4_2019'] = data['2019-09'] + data['2019-10'] + data['2019-11']
    cur_quarter = get_current_quarter(data, months)
    fy_months = get_fiscalyear(months)
    data['Revenue Requirements (Qtr)'] = data['Revenue Requirements (Annual)'] / 4
    data['YTD(SNX Fiscal)'] = 0
    for mon in fy_months:
        data['YTD(SNX Fiscal)'] = data['YTD(SNX Fiscal)'] + data[mon]
    data['Last year (SNX FY 2019)'] = data['Q1_2019'] + data['Q2_2019'] + data['Q3_2019'] + data['Q4_2019']
    start_date = datetime.date(2019, 12, 1)
    days = (datetime.date.today() - start_date).days
    data['RunRate(SNX FY2020)'] = data['YTD(SNX Fiscal)'] / (days-2) * 365
    data['AvgSales(By SNX FiscalYear)'] = (data['Q1_2017'] + data['Q2_2017'] + data['Q3_2017']
                                           + data['Q4_2017'] + data['Q1_2018'] + data['Q2_2018'] + data['Q3_2018'] +
                                           data['Q4_2018']
                                           + data['Last year (SNX FY 2019)']) / 3
    data['SNX Annnual Oppty$ (2020)'] = data['Revenue Requirements (Annual)'] - data['YTD(SNX Fiscal)']
    data['SNX Annnual Oppty$ (2020)'][data[data['SNX Annnual Oppty$ (2020)'] < 0].index] = 0
    data['QTD'] = data[cur_quarter]
    data['SNX FQ3 Oppty$'] = data['Revenue Requirements (Qtr)'] - data['QTD']
    data['SNX FQ3 Oppty$'][data[data['SNX FQ3 Oppty$'] < 0].index] = 0
    cur_month = months[-1]
    last_cur_month = '2019-' + cur_month.split('-')[1]
    data['MTD Rev$'] = data[cur_month]
    data['MoM %'] = (data['MTD Rev$'] - data[last_cur_month]) / data[last_cur_month]
    data['MoM %'][data[data[last_cur_month] == 0].index] = 0
    data['MoM %'] = data['MoM %'].fillna(0)
    cur_quarter_lastyear = cur_quarter.split('_')[0] + '_2019'
    data['Total sales in {} 2019'.format(cur_quarter.split('_')[0])] = data[cur_quarter_lastyear]
    last_quarter, last_last_quarter, last_quarter_lastyear = get_quarters(cur_quarter)
    sales1 = 'sales_' + last_quarter_lastyear
    sales2 = 'sales_' + last_last_quarter
    sales3 = 'sales_' + last_quarter
    data[sales1] = data[last_quarter_lastyear]
    data[sales2] = data[last_last_quarter]
    data[sales3] = data[last_quarter]
    data = pd.merge(data, sales_division, on='Sales Division', how='left', validate='m:1')
    data = pd.merge(data, sales_group, on='Sales Group', how='left', validate='m:1')
    data['Ref for Co'] = None
    month_num, month_name = get_month(cur_month)
    data['buying customer in {}, 2019'.format(month_name)] = data['MCN#']
    data['buying customer in {}, 2019'.format(month_name)][data[data[last_cur_month] == 0].index] = 0
    data['buying customer in {},2019'.format(cur_quarter.split('_')[0])] = data['MCN#']
    data['buying customer in {},2019'.format(cur_quarter.split('_')[0])][
        data[data[cur_quarter_lastyear] == 0].index] = 0
    data['buying customer in {}, 2020'.format(month_name)] = data['MCN#']
    data['buying customer in {}, 2020'.format(month_name)][data[data[cur_month] == 0].index] = 0
    data['buying customer in {},2020'.format(cur_quarter.split('_')[0])] = data['MCN#']
    data['buying customer in {},2020'.format(cur_quarter.split('_')[0])][data[data['QTD'] == 0].index] = 0
    data['buying vendor in 2019'] = data['Vend Name']
    data['buying vendor in 2020'] = data['Vend Name']
    data['buying vendor in 2019'][data[data['Last year (SNX FY 2019)'] == 0].index] = 0
    data['buying vendor in 2020'][data[data['YTD(SNX Fiscal)'] == 0].index] = 0
    data['Left Period'] = None
    data['Right Period'] = None

    data['Progress'] = ''
    for i in range(data['Ref'].count()):
        progress = get_progress(data['YTD(SNX Fiscal)'][i], data['Last year (SNX FY 2019)'][i],
                                data['AvgSales(By SNX FiscalYear)'][i], data['RunRate(SNX FY2020)'][i])
        data['Progress'][i] = progress

    print('working on generating new_data..........')
    new_date = pd.DataFrame()
    new_date['Progress'] = data['Progress']
    new_date['Vend Name'] = data['Vend Name']
    new_date['Partner level'] = data['Partner level']
    new_date['MCN#'] = data['MCN#']
    new_date['Reseller'] = data['Reseller']
    new_date['Terr#'] = data['Terr#']
    new_date['Sales Group'] = data['Sales Group']
    new_date['Sales Division'] = data['Sales Division']
    new_date['BD Project#'] = data['BD Project#']
    new_date['Target project#'] = data['Target project#']
    new_date['BD Rep'] = data['BD Rep']
    new_date['Has BD Coverage?'] = data['Has BD Coverage?']
    new_date['Has existed on Target reseller list?'] = data['Has existed on Target reseller list?']
    new_date['Revenue Requirements (Annual)'] = data['Revenue Requirements (Annual)']
    new_date['Revenue Requirements (Qtr)'] = data['Revenue Requirements (Qtr)']
    new_date['SNX Annnual Oppty$ (2020)'] = data['SNX Annnual Oppty$ (2020)']
    new_date['SNX FQ3 Oppty$'] = data['SNX FQ3 Oppty$']
    new_date['QTD'] = data['QTD']
    new_date['MTD Rev$'] = data['MTD Rev$']
    new_date['MoM %'] = data['MoM %']
    new_date[sales1] = data[sales1]
    new_date[sales2] = data[sales2]
    new_date[sales3] = data[sales3]
    new_date['Total sales in {} 2019'.format(cur_quarter.split('_')[0])] = data[
        'Total sales in {} 2019'.format(cur_quarter.split('_')[0])]
    new_date['YTD(SNX Fiscal)'] = data['YTD(SNX Fiscal)']
    new_date['Last year (SNX FY 2019)'] = data['Last year (SNX FY 2019)']
    new_date['RunRate(SNX FY2020)'] = data['RunRate(SNX FY2020)']
    new_date['AvgSales(By SNX FiscalYear)'] = data['AvgSales(By SNX FiscalYear)']
    new_date['Sales Division ID'] = data['Sales Division ID']
    new_date['Sales Group ID'] = data['Sales Group ID']
    new_date['Ref for Co'] = data['Ref for Co']
    new_date['buying customer in {}, 2019'.format(month_name)] = data['buying customer in {}, 2019'.format(month_name)]
    new_date['buying customer in {},2019'.format(cur_quarter.split('_')[0])] = data[
        'buying customer in {},2019'.format(cur_quarter.split('_')[0])]
    new_date['buying customer in {}, 2020'.format(month_name)] = data['buying customer in {}, 2020'.format(month_name)]
    new_date['buying customer in {},2020'.format(cur_quarter.split('_')[0])] = data[
        'buying customer in {},2020'.format(cur_quarter.split('_')[0])]
    new_date['buying vendor in 2019'] = data['buying vendor in 2019']
    new_date['buying vendor in 2020'] = data['buying vendor in 2020']
    for month in months:
        new_date[month] = data[month]
    new_date['Q1_2017'] = data['Q1_2017']
    new_date['Q2_2017'] = data['Q2_2017']
    new_date['Q3_2017'] = data['Q3_2017']
    new_date['Q4_2017'] = data['Q4_2017']
    new_date['Q1_2018'] = data['Q1_2018']
    new_date['Q2_2018'] = data['Q2_2018']
    new_date['Q3_2018'] = data['Q3_2018']
    new_date['Q4_2018'] = data['Q4_2018']
    new_date['Q1_2019'] = data['Q1_2019']
    new_date['Q2_2019'] = data['Q2_2019']
    new_date['Q3_2019'] = data['Q3_2019']
    new_date['Q4_2019'] = data['Q4_2019']
    new_date['Q1_2020'] = data['Q1_2020']
    new_date['Q2_2020'] = data['Q2_2020']
    try:
        new_date['Q3_2020'] = data['Q3_2020']
    except:
        new_date['Q3_2020'] = None
    try:
        new_date['Q4_2020'] = data['Q4_2020']
    except:
        new_date['Q4_2020'] = None
    new_date['Left Period'] = data['Left Period']
    new_date['Right Period'] = data['Right Period']

    print('working on export data to excel.............')
    new_date.to_excel('DATA.xlsx', sheet_name='DATA', index=False)


if __name__ == '__main__':
    data_path = 'E:\Projects\BI\CA report\Canada Cross sell and TAM growth updated as of 0629.xlsx'
    pos_path = 'E:/Projects/BI/CA report/07.08/POS_Online_Report.xlsx'
    customer_path = 'E:/Projects/BI/CA report/07.08/data - 2020-07-06T091741.390.xlsx'


    process_data(data_path, pos_path, customer_path)
