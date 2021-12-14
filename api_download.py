## import library
from ast import Index
from sys import argv
import requests
import datetime
from requests.api import request
import sys
from requests.models import Response
from parameter import *
from pandas.io.json import json_normalize
import pandas as pd
import json
import subprocess
from paralel import *

def sub_execute(cmd):
    prc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True, shell=True)
    stdout, stderr = prc.communicate()
    exitcode = prc.wait()
    return stdout, stderr, exitcode

try :
    paralel = sys.argv[1]
except:
    paralel=1

class ApiConnect:
    def __init__(self):
        self.yelp_key = KEY_API_YELP
        self.wheater_key = KEY_API_WHEATER
        self.hostname_yelp = 'http://api.yelp.com'
        self.hostname_weather = "http://api.openweathermap.org/data/2.5/weather"

    
    def pull(self, api_key, url,header,params):
        result =[] 
        response = requests.request('GET', url, headers=header, params=params)
        return response.json()


    def cleansing(self,df):
        df = json.dumps(df['businesses'])
        df = json.loads(df)
        parsed_df = pd.json_normalize(df)
        parsed_df.index = parsed_df['id']
        dataframe = pd.DataFrame(parsed_df,columns=COLUMN_NAME)
        # dataframe = self.clean_column(dataframe)
        dataframe.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r","\/"], value=[" "," ",""], regex=True, inplace=True)
        dataframe.rename(columns=lambda x: x.replace(' ', '_').replace(">", "").replace(".", "_").replace("<","").replace("\t", "").replace("\r", "").replace("\n", "").lower(), inplace=True)
        return dataframe

    def writedata(self,df,namefile):
        date_time = datetime.date.today().strftime("%Y%M%d")
        df.to_json(f"{filepath}{namefile}_{date_time}.json", orient="records")
        status = 'Success'
        return status

    def get_data_yelp (self,index=0):
        # Default 1000 data

        url = self.hostname_yelp + '/v3/businesses/search'
        header = {'Authorization': 'Bearer %s' % self.yelp_key}
        paramss = {
            'term': business_type,
            'location': location_search
            }
        data_raw = self.pull(api_key = self.yelp_key, url=url, header=header, params=paramss)
        df_clean = self.cleansing(data_raw)
        # print (df_clean)
        status = self.writedata(df_clean,f'restaurant_rating_{index}')
        # print (status)
    
    def get_data_wheatermap(self,index=0):
        paramss={
            'q':city_name,
            'APPID': self.wheater_key
        }
        r = requests.get(weather_url, params=paramss)

        with open(filepath+f'weathermap_{index}.json', 'a+') as f:
            json.dump(r.json(),f)





if __name__ == "__main__":
    print ("Started main program")
    print (f"You'll be download Data {business_type} from yelp.com with location {location_search} and will be saved in {filepath}")
    try :
        param = sys.argv[2]
        log_workers, exitcodes = execute_parallel("ApiConnect({}).get_data_yelp()", paralel)
        for idx, exitcode in enumerate(exitcodes):
            print ("Process in paralel {idx} status {exitcode}")
        status ='paralel'
    except Exception as e:
        ApiConnect().get_data_yelp()
        # print (e)
        status='multiple'
    print (f"Processed Done with {status} process")
    print (f"You'll be download Data from openweathermap.com with location {city_name} and will be saved in {filepath}")
    try :
        log_workers, exitcodes = execute_parallel("ApiConnect({}).get_data_wheatermap()", paralel)
        for idx, exitcode in enumerate(exitcodes):
            print ("Process in paralel {idx} status {exitcode}")
        status ='paralel'
    except Exception as e:
        ApiConnect().get_data_yelp()
        # print (e)
        status='multiple'
    print (f"Processed Done with {status} process")



