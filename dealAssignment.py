#!/usr/bin/python2.7
import requests
import boto3
import json

# Parameters to change: *BUCKET_NAME*, *FILE_NAME*, *YOUR_REFRESH_TOKEN*, *REQUEST_DEALS*

def authenticationrequest():
    # Getting previous token from S3 bucket and sending test request just to get status code 200 or 401
    # S3 bucket information
    # Using paint text file to store valid token
    s3 = boto3.resource('s3')
    object = s3.Object(*BUCKET_NAME*, *FILE_NAME*')
    latestToken = object.get()['Body'].read().decode('utf-8')
    print("Retrieved last token: "+latestToken)
    testRequest = requests.get('https://www.zohoapis.com/crm/v2/Leads', headers={"Authorization": "Zoho-oauthtoken "+latestToken})

    # It it is not 200, then generating new token using refresh token and overwriting previous one in S3 bucket
    if testRequest.status_code != 200:
        testAuth = (requests.post(
            *YOUR_REFRESH_TOKEN*)).json()
        latestToken = testAuth['access_token']
        object.put(Body=latestToken)
        print ("Previous token expired, overwriting with new one: " + latestToken)
        return latestToken
    else:
        return latestToken

def dealpagerequest(page, zohoToken):
    print ("Processing page # " + str(page))
    # Requesting p-page of deals which are assigned to *REQUEST_DEALS*
    dealrequest = (requests.get(
        'https://www.zohoapis.com/crm/v2/Potentials/search?criteria=((Owner:equals:*REQUEST_DEALS*))&page=' + str(
            page) + '&per_page=200',
        headers={"Authorization": "Zoho-oauthtoken " + zohoToken})).json()
    return dealrequest

def accountsrequest(count, zohoToken, deal, dealrequest):
    print ("Processing deal # " + str(count))
    # Parsing deal json for related account Id, deal Owner and deal Id
    account = dealrequest['data'][deal]['Account_Name']['id']
    owner = dealrequest['data'][deal]['Owner']['name']
    id = dealrequest['data'][deal]['id']
    # Based on previous section getting related account
    accountrequest = (requests.get(
        'https://www.zohoapis.com/crm/v2/Accounts/search?criteria=(id:equals:' + account + ')',
        headers={"Authorization": "Zoho-oauthtoken " + zohoToken})).json()
    return account, owner, id, accountrequest

def accountassignment (dealid, accrequest, currentaccount, zohoToken, acctoskip):
    # Parsing for account owner
    accountowner = accrequest['data'][currentaccount]['Owner']['name']
    # Check if account owner is not in to-skip-list
    changerequest = {}
    datatosend = {}
    updaterequest = 0
    if accountowner not in acctoskip:
        # Request formation 1. Array with deal Id and account owner 2. Creating valid JSON payload for zoho
        changerequest = ({'id': dealid, 'Owner': accrequest['data'][currentaccount]['Owner']})
        datatosend = {"data": [changerequest]}
        updaterequest = requests.put('https://www.zohoapis.com/crm/v2/Potentials',
                                     data=json.dumps(datatosend),
                                     headers={"Authorization": "Zoho-oauthtoken " + zohoToken,
                                              "Content-Type": "application/json"})
    return updaterequest, datatosend, accountowner

newToken = authenticationrequest()

# Counter for deals
c = 0
# Array of accounts to skip
accountsToSkip = []

# Main loop
for p in range(1, 100):
        try:
        dealRequest = dealpagerequest(p, newToken)
    except KeyError:
        newToken = authenticationrequest()
        dealRequest = dealpagerequest(p, newToken)
    # Processing portion of 200 deals one by one
    for deal in range(0, len(dealRequest['data'])):
        # Handling for deals w/o account assigned
        try:
            c += 1
                        try:
                accountId, dealOwner, dealId, accountRequest = accountsrequest(c, newToken, deal, dealRequest)
            except KeyError:
                newToken = authenticationrequest()
                accountId, dealOwner, dealId, accountRequest = accountsrequest(c, newToken, deal, dealRequest)
            # This loop could be excessive but don't want to debug this actually
            for account in range(0, len(accountRequest['data'])):
                try:
                    updateRequest, dataToSend, accountOwner = accountassignment(dealId, accountRequest, account, newToken, accountsToSkip)
                except KeyError:
                    newToken = authenticationrequest()
                    updateRequest, dataToSend, accountOwner = accountassignment(dealId, accountRequest, account, newToken, accountsToSkip)
            # Skipping deals which are assigned to someone from the list
            try:
                print ("Request: " + str(dataToSend) + '\n' + updateRequest.text + '\n' + str(updateRequest.status_code))
            except KeyError:
                newToken = authenticationrequest()
                print ("Request: " + str(dataToSend) + '\n' + updateRequest.text + '\n' + str(
                    updateRequest.status_code))
            except:
                print ('Account owner is from to-skip-list' + '\n' + str(dealId) + '\n')
        except Exception as e:
            print (e)
            print ('This deal is weird:' + str(dealRequest['data'][deal]['id']))
    # Quit loop when reaching lasp page with results
    if dealRequest['info']['count'] != 200:
        print (str(dealRequest['info']['count']))
        break
