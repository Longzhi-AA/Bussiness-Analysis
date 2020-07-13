import pandas as pd

def keep_min(df, by='Scan Date'):
    df = df.sort_values(by=by)
    df = df.drop_duplicates(subset=['WO#'], keep='first')

    return df


def keep_max(df,by='Scan Date'):
    df = df.sort_values(by=by)
    df = df.drop_duplicates(subset=['WO#'], keep='last')

    return df


def process_logi_movem(raw_data_path):
    print('''Loading raw data............''')
    raw_data_df = pd.read_excel(raw_data_path,
                                sheet_name= 'Raw data',
                                dtype={'WHS#':'str', 'WO#':'str', 'Scan Date':'datetime64[ns]'})
    raw_data_df['Route Code'] = raw_data_df['Route Code'].str.strip()
    raw_data_df['Operation'] = raw_data_df['Operation'].str.strip()
    raw_data_df = raw_data_df.drop(columns=['QC Name','Defect Code','Defect Description'])
    print('working on creating IN/OUT df.......')
    ICBTO_QC_df = raw_data_df[raw_data_df['Route Code'] == 'ICBTO QC']
    MFG_Staging_df = raw_data_df[raw_data_df['Route Code'] == 'MFG Staging']
    ICBTO_QCIN_df = ICBTO_QC_df[ICBTO_QC_df['Operation'] == 'I']
    ICBTO_QCOUT_df = ICBTO_QC_df[ICBTO_QC_df['Operation'] == 'O']
    MFG_StagingIN_df = MFG_Staging_df[MFG_Staging_df['Operation'] == 'I']
    MFG_StagingOUT_df = MFG_Staging_df[MFG_Staging_df['Operation'] == 'O']
    ICBTO_QCIN_df = keep_min(ICBTO_QCIN_df)
    ICBTO_QCOUT_df = keep_max(ICBTO_QCOUT_df)
    MFG_StagingIN_df = keep_min(MFG_StagingIN_df)
    MFG_StagingOUT_df = keep_max(MFG_StagingOUT_df)
    ICBTO_QCIN_df = ICBTO_QCIN_df.rename(columns={'Scan Date':'ICBTO QC In'})
    ICBTO_QCOUT_df = ICBTO_QCOUT_df.rename(columns={'Scan Date':'ICBTO QC Out'})
    MFG_StagingIN_df = MFG_StagingIN_df.rename(columns={'Scan Date':'MFG Staging In'})
    MFG_StagingOUT_df = MFG_StagingOUT_df.rename(columns={'Scan Date':'MFG Staging Out'})

    pure_raw_data = raw_data_df
    pure_raw_data = pure_raw_data.drop(columns=['Route Code','Operation','Scan Date'])
    pure_raw_data['Ref'] = pure_raw_data['WHS#']+'-'+pure_raw_data['WO#']
    pure_raw_data = pure_raw_data.drop_duplicates(subset='Ref')

    pure_IC_IN = ICBTO_QCIN_df[['WHS#', 'WO#', 'ICBTO QC In']]
    pure_IC_OUT = ICBTO_QCOUT_df[['WHS#', 'WO#', 'ICBTO QC Out']]
    pure_MFG_IN = MFG_StagingIN_df[['WHS#', 'WO#', 'MFG Staging In']]
    pure_MFG_OUT = MFG_StagingOUT_df[['WHS#', 'WO#', 'MFG Staging Out']]

    print('working on creating new temp data......')
    temp_data = pd.merge(pure_raw_data,pure_IC_IN,how='left', on=['WHS#','WO#'])
    temp_data = pd.merge(temp_data,pure_IC_OUT,how='left', on=['WHS#','WO#'])
    temp_data = pd.merge(temp_data,pure_MFG_IN,how='left', on=['WHS#','WO#'])
    temp_data = pd.merge(temp_data,pure_MFG_OUT,how='left', on=['WHS#','WO#'])
    temp_data = temp_data.drop(columns=['Ref'])
    temp_data['QC Name'] = None
    temp_data['Defect Code'] = None
    temp_data['Defect Description'] = None
    # temp_data.to_csv('test.csv', index=False)

    return temp_data


def append_data_to_master(raw_data_path,master_file_path):
    temp_data = process_logi_movem(raw_data_path)
    master_file = pd.read_excel(master_file_path,
                                sheet_name='Logistics Movement',
                                dtype={'WHS#':'str','WO#':'str','ICBTO QC In':'datetime64[ns]',
                                       'ICBTO QC Out':'datetime64[ns]','MFG Staging In':'datetime64[ns]','MFG Staging Out':'datetime64[ns]'})
    master_file = pd.concat([master_file,temp_data],axis=0,ignore_index=True, sort=False)
    qc_in = master_file[['WO#', 'ICBTO QC In']]
    qc_out = master_file[['WO#', 'ICBTO QC Out']]
    staging_in = master_file[['WO#', 'MFG Staging In']]
    staging_out = master_file[['WO#', 'MFG Staging Out']]
    qc_in = keep_min(qc_in, by='ICBTO QC In')
    qc_out = keep_max(qc_out, by='ICBTO QC Out')
    staging_in = keep_min(staging_in,by='MFG Staging In')
    staging_out = keep_max(staging_out, by='MFG Staging Out')

    print('working on appending to master file......')
    master = master_file[['WHS#', 'WO#','Flavor','System Type','Qty','QC Name','Defect Code','Defect Description']]
    master = master.drop_duplicates(subset=['WO#'])
    master = pd.merge(master,qc_in, how='left', on=['WO#'], validate='1:1')
    master = pd.merge(master,qc_out, how='left', on=['WO#'], validate='1:1')
    master = pd.merge(master,staging_in, how='left', on=['WO#'], validate='1:1')
    master = pd.merge(master,staging_out, how='left', on=['WO#'], validate='1:1')

    new_master = pd.DataFrame()
    new_master['WHS#'] = master['WHS#']
    new_master['WO#'] = master['WO#']
    new_master['Flavor'] = master['Flavor']
    new_master['System Type'] = master['System Type']
    new_master['Qty'] = master['Qty']
    new_master['ICBTO QC In'] = master['ICBTO QC In']
    new_master['ICBTO QC Out'] = master['ICBTO QC Out']
    new_master['MFG Staging In'] = master['MFG Staging In']
    new_master['MFG Staging Out'] = master['MFG Staging Out']
    new_master['QC Name'] = master['QC Name']
    new_master['Defect Code'] = master['Defect Code']
    new_master['Defect Description'] = master['Defect Description']



    temp_data.to_excel('weekly_data.xlsx', index=False, sheet_name='Total after dudep')
    master.to_excel('logi_movement_data.xlsx',index=False, sheet_name='Logistics Movement')
    print('*********Done! Successfully append data to master file!*********')


if __name__ == '__main__':

    raw_data_path = '/Users/longzhi/Projects/BI_download/Logistics Movement/Logistics movement_6-13-2020 to 6-30-2020.xlsx'
    master_file_path = '/Users/longzhi/Projects/BI_download/Logistics Movement/Logistics movement Data_as of 6-12-2020 for testing automation.xlsx'

    append_data_to_master(raw_data_path,master_file_path)