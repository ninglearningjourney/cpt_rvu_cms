#### Purpose of this program is to:
#### 1. Download any new files from CMS website and save them locally 
#### 2. Extract the data from the files 
#### 3. Filter on the CPT codes that we need based on ServiceLine database 
#### 4. Load the needed data into staging table 
#### 5. Call stored procedure to load the data from staging to prod 

import sys
sys.path.append('F:\Scripts\GeneralScripts')
import esi_send_message
import database_connections
import os
import urllib.request as rq
from bs4 import BeautifulSoup
import re 
from zipfile import ZipFile
import pandas as pd
from calendar import month_name
from time import strptime, sleep
from datetime import datetime


#### Preparation:

source_name = 'CPT_RVU_CMS.py'
err_email_receiver = 'xiongn@ccf.org'
err_email_subject = 'CPT_RVU_CMS.py Error'


def error_handling(err_desc, warning_desc, cursor):
    src_name = source_name
    cleaned_err_desc = err_desc.replace('\'', '"')
    sql = f"call dl_esi_prod.sp_log_error('{cleaned_err_desc}', '{warning_desc}', '{src_name}');"   
    cursor.execute(sql)
    esi_send_message.send_mail(receiver = err_email_receiver, subject = err_email_subject, message = err_desc)
    
with database_connections.td_connect() as con:
    with con.cursor() as cursor:

    

        #### Log Start
        try:
            sql = f"call dl_esi_prod.sp_Log_Process('{source_name}','Start');" 
            cursor.execute(sql)
            
        except Exception as ex:
            err_desc = f'Error occurred when logging start: {ex}'
            warning_desc = ''
            error_handling(err_desc, warning_desc, cursor)
            sys.exit(1)  


        #### Step 1: Create a list of zipfiles that have been downloaded loacally already 

        try:
            folder_path = 'F:/Scripts/CPT_RVU_CMS'    ## define folder path

            downloaded_zip_list_ext = [z for z in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, z)) and '.zip' in z]    ## list of downloaded zip files in the folder (with the .zip extension)

            downloaded_zip_list = []    ## initiate an empty list

            for zip in downloaded_zip_list_ext:
                z_name = zip.split('.')[0]    ## to get the file name without the .zip extension
                downloaded_zip_list.append(z_name)    ## append to the list

            print(f"Downloaded zip files: {downloaded_zip_list}")
            print(len(downloaded_zip_list))

        except Exception as ex:
            err_desc = f'Error occurred when getting a list of downloaded zip files: {ex}'
            warning_desc = ''
            error_handling(err_desc, warning_desc, cursor)
            sys.exit(1)    



        #### Step 2: Create a list of files that are available on the CMS website 

        try:
            cms_zip_dic = {}    ## initiate an empty dictionary 

            url1 = 'https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/PhysicianFeeSched/PFS-Relative-Value-Files?items_per_page=50'
            html = rq.urlopen(url1).read()    ## get the html content 

            soup = BeautifulSoup(html, "html.parser")    ## use BeautifulSoup to parse through the html 

            for link in soup.find_all('a'):    ## get all <a> tags in the html 
                href1 = link['href']    ## get the link after href attribute
                if 'RVU' in href1.upper() and int(re.findall(r'\d+\.\d+|\d+', href1.split('/')[-1])[0]) > 15:    ## any link with the string 'RVU' in it and after year 2015
                    name = href1.split('/')[-1]    ## get the file name by using the last portion split by '/' in the link
                    cms_zip_dic[name] = href1    ## add the file name and link to the dictionary

            print(f"Available on website: {cms_zip_dic.keys()}")
            print(len(cms_zip_dic.keys()))

        except Exception as ex:
            err_desc = f'Error occurred when getting a list of zip files available on CMS website: {ex}'
            warning_desc = ''
            error_handling(err_desc, warning_desc, cursor)
            sys.exit(1) 
            
            

        #### Step 3: Compared the two lists from Step 1 and Step 2, in order to get a list of files that need to be downloaded 

        try:
            new_zip_list = [z for z in cms_zip_dic.keys() if z not in downloaded_zip_list]    ## get a list of zip files that are available on CMS website but are not downloaded locally, by comparing the two lists 
            print(f"Need to download: {new_zip_list}")

            new_zip_list.sort()    ## sort the list to download earlier years and earlier versions first 

        except Exception as ex:
            err_desc = f'Error occurred when getting a list of zip files that need to be downloaded: {ex}'
            warning_desc = ''
            error_handling(err_desc, warning_desc, cursor)
            sys.exit(1) 
            


        #### Step 4: Execute a query to get the CPT codes that have been billed under ESI accounting units, and then load it into a DataFrame 

        try:
            cpt_sql = 'select distinct scd.CPT_HCPCS_Code from SERVICE_LINE.CHARGE_DETAIL scd join DL_ESI_PROD.ED_Accounting_Unit au on scd.Accounting_Unit_Code = au.au'    ## to retrieve the CPT codes billed under ESI accounting units per ServiceLine
            cpt_df = pd.read_sql_query(cpt_sql, con)    ## load it into a DataFrame

        except Exception as ex:
            err_desc = f'Error occurred when generating the cpt_df: {ex}'
            warning_desc = ''
            error_handling(err_desc, warning_desc, cursor)
            sys.exit(1) 
            


        #### Step 5: Iterate through each new file that needs to be downloaded: 
        #### 1) download the zipfile and save it locally; 
        #### 2) get the file path of the desired csv. file, and extract the date from it to load it into a DataFrame; 
        #### 3) subset the DataFrame to only include rows without modifiers, to only include CPT an RVU columns and to only include the CPT codes we need based on Step 4 above 
        #### 4) generate the Effective_Date for the file, and add it as a new column to the DataFrame
        #### 5) add insert_dttm as a new column to the DataFrame
        #### 6) delete all rows in the staging table
        #### 7) load the staging table
        #### 8) call stored procedure to load the prod table from the staging table 

        for new_zip in new_zip_list:
            try:
                url2 = f"https://www.cms.gov/{cms_zip_dic[new_zip]}"    ## individual web page for the new zip file 
                html2 = rq.urlopen(url2).read()    ## content of html
                
                soup2 = BeautifulSoup(html2, "html.parser")    ## parse through html 
                
                for link in soup2.find_all('a'):  ## get all <a> tags
                    href2 = link['href']    ## get the link after the href attribute
                    if '.zip' in href2:    ## any link with the string '.zip' in it 
                        url3 = f"https://www.cms.gov{href2}"    ## url for downloading the zip file directly
                
                response = rq.urlopen(url3)
                
                save_path = f'F:/Scripts/CPT_RVU_CMS/{new_zip}.zip'    ## folder path where the new zip file should be saved at 
                with open(save_path, "wb") as nz:
                    nz.write(response.read())    ## save the zip file 
            
            except Exception as ex:
                err_desc = f'Error occurred when downloading new zip file: {ex}'
                warning_desc = ''
                error_handling(err_desc, warning_desc, cursor)
                sys.exit(1) 
                
            
            with ZipFile(save_path, "r") as zf:  
                
                try:
                    file_name_list = zf.namelist()    ## get a list of file names in the zip file
                    rvu_csv_list = [f for f in file_name_list if "RVU" in f.upper() and ".csv" in f]    ## get a list of files that have 'rvu' in the name and are in csv format. there should be only one file like that
                    rvu_csv_name = rvu_csv_list[0]    ## get the file name 
                    rvu_csv_path = zf.extract(rvu_csv_name)   ## get the path for the csv file 
                    
                    rvu_csv_df = pd.read_csv(rvu_csv_path, header = None, encoding='windows-1252')    ## load the csv file into a DataFrame
                    #print(rvu_csv_df)
                    
                    first_row_str = rvu_csv_df.iloc[0, 2]   ## get the 3rd element in the first row in the csv file; ex: "2019 National Physician Fee Schedule Relative Value File January Release"
                    print(first_row_str)
                    
                    eff_yr = re.findall(r'\d+\.\d+|\d+', first_row_str)[0]    ## use regular expression to get the number in the string to obtain effective year; ex: "2019"
                    
                    try:    ## try using the first row in the csv file to get effective month
                        month_set = {m.lower() for m in month_name[1:]}    ## create a set of month names
                        eff_mth_name = next((word for word in first_row_str.split() if word.lower() in month_set), None)    ## capture month name in the string; ex: "January"
                        eff_mth_num = datetime.strptime(eff_mth_name, '%B').month    ## convert the month name into month number; ex: 1
                        eff_mth_num_double = str(eff_mth_num).zfill(2)    ## convert any single digit month number into double digit; ex: "01"
                        
                    except:    ## if the first row in the csv file cannot provide month name, then use the file name 
                        eff_mth_name = rvu_csv_name.split('_')[1][0:3]    ## get the effective month based on the file name; ex: JAN
                        eff_mth_num = strptime(eff_mth_name,'%b').tm_mon    ## convert the month name into month number
                        eff_mth_num_double = str(eff_mth_num).zfill(2)    ## convert any single digit month number into double digit 
                    
                    eff_date_str = f"{eff_yr}-{eff_mth_num_double}-01 00:00:00.000000"    ## effective date string 
                    print(eff_date_str)
                    
                    
                    rvu_csv_df = rvu_csv_df.iloc[9:]              ## skip the first 9 rows in the DataFrame
                    rvu_csv_df.columns = rvu_csv_df.iloc[0]    ## assign the first row as the column names
                    rvu_csv_df = rvu_csv_df.iloc[1:]     ## finally remove the first row to avoid duplicate column names                    
                    #print(rvu_csv_df)

                except Exception as ex:
                    err_desc = f'Error occurred when generating rvu_csv_df: {ex}'
                    warning_desc = ''
                    error_handling(err_desc, warning_desc, cursor)
                    sys.exit(1)
                    
                
                try:   
                    rvu_csv_df = rvu_csv_df[rvu_csv_df["MOD"].isnull()]   ## include rows where the column MOD is null (i.e. no modifier) 
                    cpt_rvu_df = rvu_csv_df[["HCPCS","RVU"]]    ## include only the two columns needed
                    #print(cpt_rvu_df)
                    
                    filtered_df = pd.merge(cpt_df, cpt_rvu_df.rename(columns={'HCPCS':'CPT_HCPCS_Code'}), on='CPT_HCPCS_Code', how = 'inner')    ## inner join cpt_df and cpt_rvu_df on the CPT code column
                    #print(filtered_df)
                  
                    
                    current_timestamp = datetime.now()    ## current timestamp to be used for insert_dttm
                    
                    #filtered_df["Effective_Date"] = eff_date
                    filtered_df["Effective_Date"] = eff_date_str    ## add effective date as a new column to the DataFrame
                    filtered_df["Insert_Dttm"] = current_timestamp    ## add insert_dttm as a new column to the DataFrame 
                    
                    filtered_df.columns = ['CPT_HCPCS_Code', 'RVU', 'MP_RVU', 'Effective_Date', 'Insert_Dttm']
                    filtered_df = filtered_df[['CPT_HCPCS_Code', 'RVU','Effective_Date', 'Insert_Dttm']]
                    print(filtered_df)

                except Exception as ex:
                    err_desc = f'Error occurred when generating filtered_df: {ex}'
                    warning_desc = ''
                    error_handling(err_desc, warning_desc, cursor)
                    sys.exit(1) 
                    
            
                try:
                    del_stg_sql = 'delete from DL_ESI_Staging.stg_CPT_RVU_CMS all;'    ## delete all rows in the staging table 
                    cursor.execute(del_stg_sql)
                    
                except Exception as ex:
                    err_desc = f'Error occurred when deleting rows from staging table: {ex}'
                    warning_desc = ''
                    error_handling(err_desc, warning_desc, cursor)
                    sys.exit(1)
                    
                
                try:   
                    filtered_df["CPT_HCPCS_Code"] = filtered_df["CPT_HCPCS_Code"].astype(str)    ## convert data type to string 
                    filtered_df["RVU"] = filtered_df["RVU"].astype(str)    ## ## convert data type to string 
                    #filtered_df["Effective_Date"] = filtered_df["Effective_Date"].astype(str)
                    filtered_df["Insert_Dttm"] = filtered_df["Insert_Dttm"].astype(str)    #### convert data type to string 

                    insert_sql = f"insert into DL_ESI_Staging.stg_CPT_RVU_CMS values (?,?,?,?);"    ## query to insert each row in the DataFrame into staging table 
                    cursor.fast_executemany = False
                    cursor.executemany(insert_sql, filtered_df.values.tolist())    ## convert DataFrame to list, and then batchload into staging table 
                    cursor.commit()  
                
                except Exception as ex:
                    err_desc = f'Error occurred when inserting rows into staging table: {ex}'
                    warning_desc = ''
                    error_handling(err_desc, warning_desc, cursor)
                    sys.exit(1)
                    

                try:
                    call_sp_sql = 'call dl_esi_prod.sp_cpt_rvu_cms();'    ## call stored procedure 
                    cursor.execute(call_sp_sql)
                
                except Exception as ex:
                    err_desc = f'Error occurred when calling stored procedure: {ex}'
                    warning_desc = ''
                    error_handling(err_desc, warning_desc, cursor)
                    sys.exit(1)
                    
                    
                #sleep(600)    ## pause for 600 seconds before downloading the next zip file 



        ### Log End 

        try:
            sql = f"call dl_esi_prod.sp_Log_Process('{source_name}','End');" 
            cursor.execute(sql)
            
        except Exception as ex:
            err_desc = f'Error occurred when logging end: {ex}'
            warning_desc = ''
            error_handling(err_desc, warning_desc, cursor)
            sys.exit(1)  

