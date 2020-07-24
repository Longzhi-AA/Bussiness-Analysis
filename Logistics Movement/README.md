#1.	keep_min

`keep_min(df, by='Scan Date')`

- The function “keep_min” is in order to remove duplicate items and keep the minimum Scan date;
Parameters: df - means you need put dataframe as the first parameter; by – default set as ‘Scan date’, you can use others data fields for ordering or drop duplicates.

#2.	keep_max

`keep_max(df,by='Scan Date')`

- Same to keep_min, this is to drop duplicate items and keep the maximum Scan date ;

#3.	process_logi_movem

`process_logi_movem(raw_data_path)`

- Use this function to process raw data, need to input parameter: raw data path;
- Notes: QC Name, Defect Code, Defect Description will always be null based on original procedure;

#4.	append_data_to_master

`append_data_to_master(raw_data_path,master_file_path)`

- Appending weekly data to original master file and drop duplicates;
- Parameters: raw_data_path,master_file_path;
- Weekly data result and new master file will be create via this function;
