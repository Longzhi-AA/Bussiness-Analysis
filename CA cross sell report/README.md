1.	Process_data_TAM

`process_data_TAM(data_path, customer_path)`
<pre>
- The function is in order to update and create new TAM table;
- Parameters: original TAM_path and customer dataset path
- Output: TAM table(excel)
</pre>

2.	Process_data_sales

`process_data_sales(data_path, pos_path, customer_data)`
<pre>
- The function is in order to update and create new sales table;
- Output: data_sales table(excel)
</pre>

3.	process_data

`process_data(data_path, pos_path, customer_path, sales_division, sales_group)`
<pre>
- Use this function to concate TAM and sales table, will create the DATA table, including calculations of monthly/quarterly revenue and others KPI
- Parameters: data_path, pos_path, customer_path, sales_division, sales_group
</pre>

4.	get_progress

`get_progress(x, y, z, r)`
<pre>
- Use this function to evaluate customer level( new, win-back, opportunity, existing, slipped)
</pre>
