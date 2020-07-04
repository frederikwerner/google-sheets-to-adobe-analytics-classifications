import datetime
import requests
import sys
import jwt
import re
import httplib2

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow

#Set config variables. See documentation for reference
config = {
"apiKey":"3ec174hj4k58be87e8fdivk49hkdfl396",
"technicalAccountId":"6CH54KFL43DM65FD2@techacct.adobe.com",
"orgId":"7JDB342JJJMOP78@AdobeOrg",
"secret":"dfjk3ik31-4bd2-4kf8-9df5-ff5j3834kb",
"metascopes":"ent_analytics_bulk_ingest_sdk",
"imsHost":"ims-na1.adobelogin.com",
"imsExchange":"https://ims-na1.adobelogin.com/ims/exchange/jwt",
"discoveryUrl":"https://analytics.adobe.io/discovery/me",
"key":b'-----BEGIN PRIVATE KEY-----\nMIIEv3yjqGA==\n-----END PRIVATE KEY-----',
"sheet_id":"1SfJ4k5k456lJKL4m7fkh3-KTs8",
"data_range":"Campaigns!A7:C",
"approval_range":"Campaigns!C1",
"meta_range":"Campaigns!C1:C4",
"description": "Automated Google Sheets import",
"report_suite_id": ["REPORT SUITE ID"],
"variable_id":"trackingcode",
"notification_email":"NOTIFICATION ADDRESS",
"upload_time": datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
}

#Authenticate with Google Service
def get_authenticated_google_service():
    flow = flow_from_clientsecrets("client_secrets.json", scope="https://www.googleapis.com/auth/spreadsheets",
    message="MISSING_CLIENT_SECRETS_MESSAGE")
    storage = Storage("oauth2.json")
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage)
    return build("sheets", "v4", http=credentials.authorize(httplib2.Http()))

#Build Sheets API and get data from configured ranges
google_sheets = get_authenticated_google_service().spreadsheets()
data_rows = google_sheets.values().get(spreadsheetId=config["sheet_id"], range=config["data_range"]).execute()
upload_flag = google_sheets.values().get(spreadsheetId=config["sheet_id"], range=config["approval_range"]).execute()

#Check if the file is ready to be uploaded. If not, exit and update last checked date.
if upload_flag["values"][0][0]=="No":
    print("File not ready for upload. Exiting...")
    google_sheets.values().update(spreadsheetId=config["sheet_id"], range="Campaigns!C2",valueInputOption	
="USER_ENTERED",body={"values":[[config["upload_time"]]]}).execute()
    sys.exit()

#Iterate result to create data structure for Adobe Analytics API. Change if your file structure does not match your classifciation structure
classification_rows = []
for row in data_rows["values"]:
    classification_rows.append({"row":row})

#Exit if there is no data
if len(classification_rows) < 1:
    print("No data to upload. Exiting...")
    sys.exit()

#Authenticate with the Adobe Analytics API
def get_jwt_token(config):
    return jwt.encode({
        "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=30),
        "iss": config["orgId"],
        "sub": config["technicalAccountId"],
        "https://{}/s/{}".format(config["imsHost"], config["metascopes"]): True,
        "aud": "https://{}/c/{}".format(config["imsHost"], config["apiKey"])
    }, config["key"], algorithm='RS256')

def get_access_token(config, jwt_token):
    post_body = {
        "client_id": config["apiKey"],
        "client_secret": config["secret"],
        "jwt_token": jwt_token
    }

    response = requests.post(config["imsExchange"], data=post_body)
    return response.json()["access_token"]

def get_first_global_company_id(config, access_token):
    response = requests.get(
        config["discoveryUrl"],
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apiKey"]
        }
    )
    return response.json().get("imsOrgs")[0].get("companies")[0].get("globalCompanyId")

jwt_token = get_jwt_token(config)
access_token = get_access_token(config, jwt_token)
global_company_id = get_first_global_company_id(config, access_token)

#Create Classifcations import Job. Change header for your needed Classifcation fields.
jobid = requests.post(
        "https://api.omniture.com/admin/1.4/rest/?method=Classifications.CreateImport",
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apiKey"],
            "x-proxy-global-company-id": global_company_id
        }, 
        json={
            'rsid_list':config["report_suite_id"],
            "element":config["variable_id"],
            "check_divisions":0,
            "description":config["description"],
            "email_address":config["notification_email"],
            "export_results":0,
            "header":["Key","Channel","Campaign"],
            "overwrite_conflicts":1
        }
    ).json()["job_id"]

#Add rows to import job. If you import more than 25,000 rows at once, you need to create multiple jobs
result = requests.post(
        "https://api.omniture.com/admin/1.4/rest/?method=Classifications.PopulateImport",
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apiKey"],
            "x-proxy-global-company-id": global_company_id
        }, 
        json={
            'job_id':jobid,
            'page':1,
            'rows':classification_rows
        }
    ).json()
    
#Finally, commit the import job
result = requests.post(
        "https://api.omniture.com/admin/1.4/rest/?method=Classifications.CommitImport",
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apiKey"],
            "x-proxy-global-company-id": global_company_id
        }, 
        json={
            'job_id':jobid
        }
    ).json()

#Last, update the Google Sheet to include data about the upload and reset the approval switch.
google_sheets.values().update(spreadsheetId=config["sheet_id"], range=config["meta_range"],valueInputOption	
="USER_ENTERED",body={"values":[
    ["No"],
    [config["upload_time"]],
    [config["upload_time"]],
    [len(classification_rows)]
    ]}).execute()
