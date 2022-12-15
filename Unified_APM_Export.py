import json
import csv 
import requests
import sys

from requests.exceptions import HTTPError

LIST_ACCOUNTS_US = [1]
LIST_ACCOUNTS_EU = [1943486]
NERDGRAPH_URL_US = 'https://api.newrelic.com/graphql'
NERDGRAPH_URL_EU = 'https://api.eu.newrelic.com/graphql'

USER_KEY = '<TRUNCATED>'

def _sendRequest(url, apiKey, method="GET", payload={}):

       headers = {

           'API-Key': apiKey,
           'content-type': "application/json",
       }

       try:
           print(f'Sending {method} request to {url}')
           if not payload:
               response = requests.request(
                   method, url, headers=headers)
           else:
               response = requests.request(
                   method, url, headers=headers, data=json.dumps(payload))
           response.raise_for_status()
           if method != "DELETE":
               return json.loads(response.text)
       except HTTPError as http_err:
           print(f'_sendGraphQLRequest() HTTP error : {http_err}')
           print(f'Error: {response.text}')
           raise Exception(http_err)
       except Exception as err:
           print(f'_sendGraphQLRequest() error occurred: {err}')
           raise Exception(err)
 

def nerdQuery():

   return '''query($id: Int!, $query: Nrql!){
               actor {
                   account(id: $id) {
                       nrql(timeout: 120, query: $query) {
                       results
                       }
                   }
               }
           }'''
 

def getData(accountId, timeRange, nerdGraphURL, apiKey):

   data = []
   variables = {'id': accountId, 'query': f"FROM JvmMetadataSummary, PhpMetadataSummary, DotnetMetadataSummary, NodeMetadataSummary, GoMetadataSummary, PythonMetadataSummary, RubyMetadataSummary SELECT min(sendingAccount), uniqueCount(applicationId), percentage(uniqueCount(applicationId), WHERE (eventType() = 'JvmMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 7 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 7 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 7 )) OR (eventType() = 'PythonMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 7 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 7 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 16 )) OR (eventType() = 'RubyMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 8 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 8 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 7 )) OR (eventType() = 'PhpMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 10 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 10 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 3 )) OR (eventType() = 'GoMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 3 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 3 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 20 ))  OR (eventType() = 'NodeMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 8 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 8 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 16 )) OR (eventType() = 'DotnetMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 9 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 9 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 8 ))) as 'pre-LiC %' facet sendingAccount limit max {timeRange}"}

   payload = {'query': nerdQuery() , 'variables': variables}

   while True:
        response = _sendRequest(nerdGraphURL, apiKey, method="POST", payload=payload) 
        try:
            if not response['data']['actor']['account']['nrql']['results']:
                break
            for obj in response['data']['actor']['account']['nrql']['results']:
                data.append((obj['sendingAccount'], obj['uniqueCount.applicationId'], obj['pre-LiC %']))
        except TypeError:
            print('Error while trying to fetch data. Trying again...')
            print(variables['query'])
            print(TypeError)
            continue

        lastMetric = response['data']['actor']['account']['nrql']['results'][-1]['sendingAccount']
        lastMetric = lastMetric.replace("'", r"\'")
        variables = {'id': accountId, 'query': f"FROM JvmMetadataSummary, PhpMetadataSummary, DotnetMetadataSummary, NodeMetadataSummary, GoMetadataSummary, PythonMetadataSummary, RubyMetadataSummary SELECT min(sendingAccount), uniqueCount(applicationId), percentage(uniqueCount(applicationId), WHERE (eventType() = 'JvmMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 7 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 7 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 7 )) OR (eventType() = 'PythonMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 7 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 7 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 16 )) OR (eventType() = 'RubyMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 8 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 8 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 7 )) OR (eventType() = 'PhpMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 10 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 10 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 3 )) OR (eventType() = 'GoMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 3 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 3 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 20 ))  OR (eventType() = 'NodeMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 8 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 8 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 16 )) OR (eventType() = 'DotnetMetadataSummary' and (numeric(capture(agentVersion, r'(?P<majorVersion>\d+)\..*')) < 9 ) or (numeric(capture(agentVersion, r'(?P<majorVersion>[\d]+)\..*')) = 9 and numeric(capture(agentVersion, r'\d+\.(?P<minorVersion>\d+)\..*')) < 8 ))) as 'pre-LiC %' where sendingAccount > {lastMetric} facet sendingAccount limit max {timeRange}"}
        payload = {'query': nerdQuery() , 'variables': variables}

   return data



def main(): 

    apiKey_US = '' #US
    apiKey_EU = '' #EU

    timeRange = "SINCE '2022-11-30 00:00:00 PDT' UNTIL '2022-12-07 00:00:00 PDT'"
 
    for accountId in LIST_ACCOUNTS_US:
        metrics = getData(accountId, timeRange, NERDGRAPH_URL_US, apiKey_US)
        file = open('UnifiedAPM_US.csv', 'w+', newline ='') 
        with file:     
            write = csv.writer(file) 
            write.writerows(metrics) 
        file.close()
        
    for accountId in LIST_ACCOUNTS_EU:
        metrics = getData(accountId, timeRange, NERDGRAPH_URL_EU, apiKey_EU)
        file = open('UnifiedAPM_EU.csv', 'w+', newline ='') 
        with file:     
            write = csv.writer(file) 
            write.writerows(metrics) 
        file.close()
 

if __name__ == '__main__':

   main()