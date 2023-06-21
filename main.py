from bs4 import BeautifulSoup
import requests
from icecream import ic
import pandas as pd

result = []

for year in [2021, 2022, 2023]:
    for month in [i+1 for i in range(12)]:
        for date in [i+1 for i in range(31)]:

            dateformat = f'{year}{month:02d}{date:02d}'
            url = f'https://www.timeanddate.com/scripts/cityajax.php?n=indonesia/bandung&mode=historic&hd={dateformat}&month={month}&year={year}'
            
            response = requests.get(url)

            if 'No data available' in response.text:
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')

            for index, tr in enumerate(soup.tbody.find_all('tr')):
                entry = {
                    "date": f'{year}-{month:02d}-{date:02d}',
                }
                th = tr.find('th')
                tds = tr.find_all('td')
                
                entry["temperature"] = tds[1].text.encode('ascii', 'ignore').decode().replace('C', '')
                entry["weather"] = tds[2].text
                entry["wind"] = tds[3].text
                entry["humidity"] = tds[5].text.replace('%', '')
                entry["barometer"] = tds[6].text
                entry["visibility"] = tds[7].text.encode('ascii', 'ignore').decode().replace('km', '')
                entry["time"] = th.text if index != 0 else th.text[0:5]
                
                result.append(entry)

            ic(f'{dateformat} done')

df = pd.DataFrame(result)
df.to_parquet('result.parquet', engine='pyarrow', compression='gzip')
df.to_csv('result.csv', index=False)
