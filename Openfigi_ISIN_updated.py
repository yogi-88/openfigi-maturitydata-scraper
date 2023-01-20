import re
import json
import urllib.request
import urllib.parse
import csv
from urllib import response
from urllib import error
import requests
from time import sleep
import time
from datetime import datetime
import sys
import pandas as pd

'''
See https://www.openfigi.com/api for more information.
'''

def map_jobs(reqData):
    while True:
        req = [reqData]
        print(req)
        handler = urllib.request.HTTPHandler()
        opener = urllib.request.build_opener(handler)
        openfigi_url = 'https://api.openfigi.com/v2/mapping'
        request = urllib.request.Request(openfigi_url, data=json.dumps(req).encode('utf-8'))

        request.add_header('Content-Type','application/json')

        #request.add_header('X-OPENFIGI-APIKEY', openfigi_apikey)
        request.get_method = lambda: 'POST'
        try:
            connection = opener.open(request)
        except error.HTTPError as e:
            if e.__dict__['code'] == 429:
                print (f'HTTP response code : 429 encoutered for {reqData}. Retrying req after 20 sec')
                time.sleep(20)
                continue
        if connection.code != 200:
            raise Exception('Bad response code {}'.format(str(response.status_code)))
        time.sleep(10)
        return json.loads(connection.read().decode('utf-8'))


outputdata = []
dateTimeObj = datetime.now()
filename = f'Openfigimaturity_{dateTimeObj.strftime("%d%m%Y-%H%M")}.xlsx'
def job_results_handler(jobs):

    for job in jobs:
        value = ""
        exchange = ""
        security_type = ""
        market_sector = ""
        maturity_date = ""
        name = ""
        bbg = ""
        code = job['idValue']
        print(code)
        result = map_jobs(job)[0]
        print(result)

        try:
            value = result['data'][0]['ticker']
            print(value)
            if "PERP" in value:
                maturity_date = "PREP"
            elif len(re.findall(r'\d{2}/\d{2}/\d{2}', value)) <= 0:
                maturity_date = "Not Available"
            elif len(re.findall(r'\d{2}/\d{2}/\d{4}', value)):
                maturity_date = datetime.strptime(re.findall(r'\d{2}/\d{2}/\d{4}', value)[0], '%m/%d/%Y').strftime(
                    '%d/%m/%Y')
            else:
                maturity_date = datetime.strptime(re.findall(r'\d{2}/\d{2}/\d{2}', value)[0], '%m/%d/%y').strftime(
                    '%d/%m/%Y')

        except IndexError and KeyError as e:
            None
        try:
            exchange = result['data'][0]['exchCode']
        except IndexError and KeyError as e:
            None

        try:
            name = result['data'][0]['name']
        except IndexError and KeyError as e:
            None

        try:
            security_type = result['data'][0]['securityType']
        except IndexError and KeyError as e:
            None

        try:
            market_sector = result['data'][0]['marketSector']
        except IndexError and KeyError as e:
            None
        try:
            bbg = result['data'][0]['figi']
        except IndexError and KeyError as e:
            None

        tempdata = {'Identifier': code,
                    'query': job,
                    'Bloomberg Code': bbg,
                    'Issuer Name': name,
                    'Ticker_Description': value,
                    'maturity_date': maturity_date,
                    'Exchange': exchange,
                    'Security Type': security_type,
                    'Market Sector': market_sector,
                    }

        outputdata.append(tempdata)
        df = pd.DataFrame.from_dict(outputdata)
        df.to_excel(filename, index=False)


def main():
    job_results_handler(jobs)


if __name__ == "__main__":
    Inputfile = open('OpenfigiISIN.txt', 'r')
    ISINdataset = Inputfile.readlines()

    jobs = []
    for ISIN in ISINdataset:
        job_dict = {'idType': 'ID_ISIN', 'idValue': ISIN.strip()}

        jobs.append(job_dict)

    openfigi_apikey = ''  # Put API Key here
    #x = txt.split(", ")
    main()
    Inputfile.close()



