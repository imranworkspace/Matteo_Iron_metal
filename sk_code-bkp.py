import pdb
import requests
import re
import pandas as pd
from datetime import datetime

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'priority': 'u=1, i',
    'referer': 'https://matteo-iron.com/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
}

def get_currency_price_unit(price_str):
    match = re.match(r'(\$)(\d+\.\d+|\d+)(.*)', price_str)
    if match:
        currency = match.group(1).strip() # for "$" symbol
        price_value = match.group(2).strip() # contain only amount 
        unit_regex_var = match.group(3).strip() # for "lb" and "lb and up"
        if unit_regex_var=='lb.':
            unit_regex_var='lb'
        elif unit_regex_var=='lb. & Up':
            unit_regex_var='lb & Up'
        units = unit_regex_var.strip()
    else:
        currency = ""
        price_value = ""
        units = ""
        return [currency, price_value, units]
    
rawFileName='myfile.xlsx'
marketName='155'
getUrlToScrape='https://matteo-iron.com/wp-admin/admin-ajax.php'
params = {
    'action': 'wp_ajax_ninja_tables_public_action',
    'target_action': 'get-all-data',
    'default_sorting': 'new_first',
    'skip_rows': '0',
    'limit_rows': '0',
    'ninja_table_public_nonce': 'ec6d76ed8d',
}
TABLE_ID_LST=['154','155']
# pdb.set_trace()
df_li = [] # create empty list for dataframe
for tbl_id in TABLE_ID_LST:
    params['table_id'] = tbl_id
    # pdb.set_trace()
    response = requests.get(getUrlToScrape, headers=headers, params=params)
    if response.status_code == 200:
        payload = response.json()
        details = filter(None, [d.get('value') for d in payload])
        df = pd.DataFrame(details)  
        df.drop('___id___', axis=1, inplace=True)
        df.columns = [col.lower() for col in df.columns]        
        df['Publicationdate'] = datetime.now().strftime("%Y-%m-%d")
        df.columns = [''] * len(df.columns)
        df_li.append(df.copy())  
    else:
        print("GET request failed!")
# only for price
'''prices_df = pd.DataFrame([get_currency_price_unit(pricestr) for pricestr in df['price']], columns=['Currency','Price','Units']) 
# This line creates a new dataframe called newdf by concatenating two existing dataframes (df and prices_df) side by side along the columns (axis=1).
newdf = pd.concat([df, prices_df], axis=1)
# This line drops the column named 'price' from the newdf dataframe along the columns (axis=1).
newdf.drop('price', axis=1, inplace=True)
df_li.append(newdf.copy())  
#  Concatenate the DataFrames along rows (axis=0)
'''

print(df_li)
