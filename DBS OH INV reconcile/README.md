##1.	DBS reconcile_1.py

* You can use this py script cleaning data which download from DBS. That is to say, you need to down all required data from DBS, and rename the excel file with location name “_AWS CVG” or “_CVG”, and add them to folders inbound/onhand inventory/outbound. 

* By running this script, system will create three csv files( receipt_total, oh_total,ship_total)

##2.	DBS reconcile_2.py

* You can use this script merge DBS data and Hyve data, you have get DBS data via DBS reconcile_1.py, so you still need to get Hyve data and add it to folder HYVE data (Hyve data should be csv file as well)

* By running this script, the merged data will be added to dataset, then you can use the refreshed dataset to update PBI report

* By running DBS reconcile_2.py, the new dataset will replace the old one, therefore this process also created a copy for you in case any errors when running this process.

##3.   DBS reconcile_3.py

* For additional requirements on phase II, by running this script, system will create a new folder named phase2, including all inbound/oh inventory/outbound data. 

* If you just want to refresh DBS OH INV PBI report, then no need to run this script.

