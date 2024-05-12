import requests # type: ignore
import pandas as pd
import re
import time
from datetime import datetime
# Get the current system date
current_date = datetime.now().strftime("%Y-%m-%d")

def convert_df(payload,flag):
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
    df.to_excel(f"table_{flag}.xlsx", index=False)
    print(f"Excel file 'table_{flag}.xlsx' has been created successfully.")
    print()
cookies = {
    '_ga': 'GA1.1.1952895388.1714373686',
    '_ga_3KJ106DBN7': 'GS1.1.1714384619.2.1.1714384872.0.0.0',
}
headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'priority': 'u=1, i',
    'referer': 'https://matteo-iron.com/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
}
params1 = {
    'action': 'wp_ajax_ninja_tables_public_action',
    'table_id': '154', # for table 1
    'target_action': 'get-all-data',
    'default_sorting': 'new_first',
    'skip_rows': '0',
    'limit_rows': '0',
    'ninja_table_public_nonce': 'ec6d76ed8d',
}
#for url2
params2 = {
    'action': 'wp_ajax_ninja_tables_public_action',
    'table_id': '155',# for table 2
    'target_action': 'get-all-data',
    'default_sorting': 'old_first',
    'skip_rows': '0',
    'limit_rows': '0',
    'ninja_table_public_nonce': 'ec6d76ed8d',
}

BASE_URL = 'https://matteo-iron.com'
URL='https://matteo-iron.com/wp-admin/admin-ajax.php'
print('loading..')
response1 = requests.get(URL, params=params1, cookies=cookies, headers=headers)
response2 = requests.get(URL, params=params2, cookies=cookies, headers=headers)
res_lst = [response1,response2]
for index, response in enumerate(res_lst,start=1):
    if response.status_code == 200:
        payload = response.json()
        convert_df(payload,index)
    