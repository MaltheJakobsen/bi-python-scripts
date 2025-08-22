import requests
import msal
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import date, timedelta, datetime
import time
import dotenv
import os


dotenv.load_dotenv()

def saveToLake(dir, filename, content):
    storage_account_name = "sinfuldatalake"
    storage_account_key = os.getenv("storage_account_key")
    container_name = "powerbiactivitylog"
    directory_name = dir  # "reviews"
    service_client = DataLakeServiceClient(
        account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name),
        credential=storage_account_key)
    file_system_client = service_client.get_file_system_client(file_system=container_name)
    dir_client = file_system_client.get_directory_client(directory_name)
    dir_client.create_directory()
    data = content
    file_client = dir_client.create_file(filename)
    file_client.append_data(data, 0, len(data))
    file_client.flush_data(len(data))

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

deltadays = 1

End_date   = date.today() - timedelta(days=deltadays)
Start_date = date.today() - timedelta(days=deltadays)

for single_date in daterange(Start_date, End_date):
    print(single_date.strftime("%Y-%m-%d"))

    #Get yesterdays date and convert to string
    activityDate1 = single_date
    activityDate1 = activityDate1.strftime("%Y-%m-%d")
    client_secret = os.getenv("client_secret")
    client_id = "cc706475-bf2b-4a13-a9e4-d20c65836505"
    authority_url = "https://login.microsoftonline.com/646c489d-96bb-4692-935f-9a97b18faf8c"
    scope = ["https://analysis.windows.net/powerbi/api/.default"]


    #Set Power BI REST API to get Activities for today
    url = f"https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime='{activityDate1}T00:00:00'&endDateTime='{activityDate1}T23:59:59'"
    #Use MSAL to grab token
    app = msal.ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)
    result = app.acquire_token_for_client(scopes=scope)
    print(result)

    # Get latest Power BI Activities
    if 'access_token' in result:
        access_token = result['access_token']
        header = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}
        api_call = requests.get(url=url, headers=header)
        # Specify empty Dataframe with all columns
        column_names = ['Id', 'RecordType', 'CreationTime', 'Operation', 'OrganizationId', 'UserType', 'UserKey',
                        'Workload', 'UserId', 'ClientIP', 'UserAgent', 'Activity', 'IsSuccess', 'RequestId', 'ActivityId',
                        'ItemName', 'WorkSpaceName', 'DatasetName', 'ReportName', 'WorkspaceId', 'ObjectId', 'DatasetId',
                        'ReportId', 'ReportType', 'DistributionMethod', 'ConsumptionMethod']
        df = pd.DataFrame(columns=column_names)

        # Set continuation URL
        contUrl = api_call.json()['continuationUri']

        # Get all Activities for first hour, save to dataframe (df1) and append to empty created df
        result = api_call.json()['activityEventEntities']
        df1 = pd.DataFrame(result)

        pd.concat([df, df1])

        # Call Continuation URL as long as results get one back to get all activities through the day
        while contUrl is not None:
            api_call_cont = requests.get(url=contUrl, headers=header)
            contUrl = api_call_cont.json()['continuationUri']
            result = api_call_cont.json()['activityEventEntities']
            df2 = pd.DataFrame(result)
            df = pd.concat([df, df2])

        # Set ID as Index of df
        df = df.set_index('Id')
        df = df.drop(['UserAgent'], axis=1)

        saveToLake("PowerBIActivity", str(activityDate1) + 'PowerBIActivity.csv', pd.DataFrame.to_csv(df))
        time.sleep(1)