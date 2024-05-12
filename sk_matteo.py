import requests
import pandas as pd
import re
import ast
from datetime import datetime
from inspect import currentframe 
import time, sys, os
import numpy as np
from scrapy import Selector
import xlsxwriter
import pytz
import json
from src.modules.utility.Fundalytics_AdditionalFieldCheck import AdditionalFieldCheck
from src.modules.utility.Fundalytics_Utility import log_moniter, log,s3FileUpload
from src.modules.utility.Fundalytics_DateModule import date_function
from src.modules.utility.Fundaltyics_MergingFiles import getFileDatetime,function_togive_singlefile_toupload

"""
def convert_df(payload):
    current_date = datetime.now().strftime("%Y-%m-%d")
    cleaned_payload = [{'material': item['value']['material'], 'price': item['value']['price']} for item in payload]
    # Extract currency, price, and units using regular expressions
    currency = []
    price_value = []
    units = []
    for item in cleaned_payload:
        price_str = item['price']
        match = re.match(r'(\$)(\d+\.\d+|\d+)(.*)', price_str)
        if match:
            currency.append(match.group(1))
            price_value.append(match.group(2))
            unit_regex_var = match.group(3).strip()
            if unit_regex_var=='lb.':
                unit_regex_var='lb'
            elif unit_regex_var=='lb. & Up':
                unit_regex_var='lb & Up'
            units.append(unit_regex_var)
        else:
            currency.append(None)
            price_value.append(None)
            units.append(None)
    
    df = pd.DataFrame({'Material': [item['material'] for item in cleaned_payload],
                    'Currency': currency,
                    'Price': price_value,
                    'Units': units,
                    'Publicationdate':current_date})
    return df


   
def get_content(dataSourceId, getUrlToScrape, dataSourceName, marketName, scraperType, spName, rawFileDateTime,
                tempFilePath, control, scraperParameterName):
    try:
        rawFileName = tempFilePath + str(rawFileDateTime).replace('-', '_') + ".xlsx"
        headers = eval(control.get(scraperParameterName, 'E_METHOD').split('|')[1])# 
        params =  eval(control.get(scraperParameterName, 'TYPE_LOCATION'))
        TABLE_ID_LST =  eval(control.get(scraperParameterName, 'TABLE_ID'))
        for tbl_id in TABLE_ID_LST:
            # Update the 'table_id' parameter with the current table ID
            params['table_id'] = tbl_id
            # Make the GET request with updated params
            response = requests.get(getUrlToScrape, headers=headers, params=params)
            df2 = pd.DataFrame()
            if response.status_code == 200:
                payload = response.json()
                if tbl_id == '154':
                    df1 = convert_df(payload)
                else:
                    df2 = convert_df(payload)
                concatenated_df = pd.concat([df1, df2], ignore_index=True)                
                concatenated_df.to_excel(rawFileName,sheet_name=marketName.lower(),index=False)  
            else:
                print("GET request failed!")
        return rawFileName
    except Exception as e:
        print('e..', e, str(sys.exc_info()[2].tb_lineno))
        log(dataSourceId, 'Extract', str(e) + ' line no: ' + str(sys.exc_info()[2].tb_lineno),
            'Error', '', control)
        sys.exit()
"""

def get_currency_price_unit(price_str):
    import pdb
    pdb.set_trace()
    
    # This part of the pattern matches a literal dollar sign ('$'). The dollar sign is a special character in regular expressions, so it's preceded by a backslash 
    #  This part of the pattern matches a decimal number. It consists of two alternatives separated by the vertical bar (|).
    #he dot (.) matches any character except newline characters (\n), and * indicates zero or more occurrences.
    match = re.match(r'(\$)(\d+\.\d+|\d+)(.*)', price_str)
    if match:
        currency = match.group(1).strip() # for "$" symbol
        price_value = match.group(2).strip() # contain only amount 
        unit_regex_var = match.group(3).strip() # for "lb" and "lb and up"
        # if unit_regex_var=='lb.':
        #     unit_regex_var='lb'
        # elif unit_regex_var=='lb. & Up':
        #     unit_regex_var='lb & Up'
        units = unit_regex_var.strip()
    else:
        currency = ""
        price_value = ""
        units = ""
    
    return [currency, price_value, units] # returned filtered currency,price_value and units
    

def get_content(dataSourceId, getUrlToScrape, dataSourceName, marketName, scraperType, spName, rawFileDateTime,
                tempFilePath, control, scraperParameterName):
    try:
        rawFileName = tempFilePath + str(rawFileDateTime).replace('-', '_') + ".xlsx"
        headers = eval(control.get(scraperParameterName, 'E_METHOD').split('|')[1])# 
        params =  eval(control.get(scraperParameterName, 'TYPE_LOCATION'))
        # get Table ids from  matteoiron.ini file inside "TABLE_ID" parameter
        TABLE_ID_LST =  eval(control.get(scraperParameterName, 'TABLE_ID')) 
        df_li = [] # create empty list for dataframe
        for tbl_id in TABLE_ID_LST:
            # Update the 'table_id' parameter with the current table ID
            params['table_id'] = tbl_id
            # Make the GET request with updated params
            response = requests.get(getUrlToScrape, headers=headers, params=params)
            if response.status_code == 200:
                # load json response into payload
                payload = response.json()
                # import pdb
                # pdb.set_trace()
                # from payload dictionary get value by 'value' key using below list comprehension, use filter function to filter values if not present return none
                # stored all filtered records into details varialble 
                details = filter(None, [d.get('value') for d in payload])
                # create dataframe using details filtered list
                df = pd.DataFrame(details)  
                # drop id from df              
                df.drop('___id___', axis=1, inplace=True)
                # make column names into lower case from lower() using in list comprehension
                df.columns = [col.lower() for col in df.columns]
                # price value devided into three column section Currency '$',Price will amount,Units will be 'lb' or 'lb & Up'
                
                prices_df = pd.DataFrame([get_currency_price_unit(pricestr) for pricestr in df['price']], columns=['Currency','Price','Units']) 
                # This line creates a new dataframe called newdf by concatenating two existing dataframes (df and prices_df) side by side along the columns (axis=1).
                newdf = pd.concat([df, prices_df], axis=1)
                # This line drops the column named 'price' from the newdf dataframe along the columns (axis=1).
                newdf.drop('price', axis=1, inplace=True)
                # create Publicationdate column using YYYY-MM-DD format
                newdf['Publicationdate'] = datetime.now().strftime("%Y-%m-%d")
                # input(newdf)
                # This appends the copied DataFrame (newdf.copy()) to the list df_li.
                # df_li becomes a list containing one DataFrame, which is a copy of newdf.
                df_li.append(newdf.copy())  
            else:
                print("GET request failed!")
                sys.exit()
        # concatenate dataframe using df_li list        
        concatenated_df = pd.concat(df_li, ignore_index=True)     
        # convert concatenated_df into excel          
        concatenated_df.to_excel(rawFileName,sheet_name=marketName.lower(),index=False)
        return rawFileName
    except Exception as e:
        print('e..', e, str(sys.exc_info()[2].tb_lineno))
        log(dataSourceId, 'Extract', str(e) + ' line no: ' + str(sys.exc_info()[2].tb_lineno),
            'Error', '', control)
        sys.exit()
        
def main(control):
    try:
        '''Get all the arguments from scrapy'''
        dataSourceId = control.dataSourceId
        dataSourceName = control.dataSourceName
        marketName = control.marketName
        spName = control.scraperParameters.split('/')
        cf = currentframe()
        '''s3path_upload, rawFileDateTime, tempFilePath = function_to_get_s3rawFilePath(control, dataSourceName,marketName)'''
        fileList = []
        rawFile = ''
        scraperParameterNameDict = {}
        s3File = None

        '''looping through the scraper parameter name'''
        for scraper_parameters in spName:
            try:
                rawFileDateTime, tempFilePath = getFileDatetime(control, dataSourceName, marketName)
                s3path_upload=control.get('s3config','s3rawfilePath')
                scraperParameterName = 'E-' + str(dataSourceName) + '-' + str(marketName) + '-' + str(
                    scraper_parameters).strip()
                '''getting url from scraperparameter'''
                getUrlToScrape = control.get(scraperParameterName, 'URL') # rename TempgetUrlToScrapDate [for token only]
                scraperType = control.get(scraperParameterName, 'SCRAPERTYPE').lower()
                getUrlToScrape = date_function(dataSourceId, getUrlToScrape, control, dataSourceName, marketName)
                s3File = get_content(dataSourceId, getUrlToScrape, dataSourceName, marketName, scraperType, spName,
                                          rawFileDateTime, tempFilePath, control, scraperParameterName)
                if s3File:
                    fileList.append(s3File)
                    scraperParameterNameDict[scraperParameterName] = [rawFile]                
            except Exception as e:
                # print("Exception::" + str(e) + " on line number " + str(sys.exc_info()[2].tb_lineno) + " for " + str(control.dataSourceId))
                log_moniter(dataSourceId, str(cf.f_lineno), str("Expected file is not found|" + str(dataSourceName)),
                    control.get('path', 'LogPath'))         
        if s3File:
            try:
                AdditionalFieldCheck(dataSourceId, dataSourceName, marketName, control, s3File)
            except Exception as e:
                print('===>',e)
                pass
            s3FileUpload(s3File, dataSourceId, s3path_upload, 'Extract', control)
            log_moniter(dataSourceId, str(cf.f_lineno), "Extraction : Completed |" + str(s3path_upload),
                        control.get('path', 'LogPath'))
            log(dataSourceId, 'Extract-Module', '', 'Extracted',
                str(s3path_upload) + str(str(s3File).replace(str(control.get('path', 'TempFilePath')), '')), control)
            control.add_section('status')##
            control.add_section('filename')##
            control.set("filename", "extractfilename",str(s3File).replace(str(control.get('path', 'TempFilePath')), ''))
            
            ''' try:
                 os.remove(str(control.get('path','TempFilePath')))
             except:
                 pass
             try:
                 for filename in fileList:
                     os.remove(filename)
             except:
                 pass'''
            control.set("status", "extractStatus", "2") ## 1 for transformation(cooked file) and 2 for extraction (raw file)
            print("Extraction Completed for " + str(dataSourceId))
            log_moniter(dataSourceId, str(cf.f_lineno), "Extraction : Success |" + str(s3File),
                        control.get('path', 'LogPath'))
            return control
        else:
            control.add_section('status')
            log_moniter(dataSourceId, str(cf.f_lineno), "Extraction : Error ",
                        control.get('path', 'LogPath'))
            control.set("status", "extractStatus", "0") ##
            log(dataSourceId, 'Extract', 'File Error on extraction', 'Error', '', control)
    except Exception as e:
        print("Exception::" + str(e) + " on line number " + str(sys.exc_info()[2].tb_lineno) + " for " + str(
            control.dataSourceId))
        log_moniter(control.dataSourceId, str(sys.exc_info()[2].tb_lineno), "Extraction : Error |" + str(e),
                    control.get('path', 'LogPath'))
        log(control.dataSourceId, 'Extract', str(e) + ' line no: ' + str(sys.exc_info()[2].tb_lineno), 'Error', '',
            control)
        return control







