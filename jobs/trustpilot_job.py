import requests
import json
import io
import pandas as pd


access_token_url = 'https://api.trustpilot.com/v1/oauth/oauth-business-users-for-applications/accesstoken'

body = {
    'grant_type' :"password",
    'username' : "chris.gilbert@bennetts.co.uk",
    'password' : "Trustpilotm1"
}

headers = {
        "Accept":"application/x-www-form-urlencoded",
        "Authorization":"MG1GZ2ZLQVpJR3YzWkd5YkUzelp2anNpMGZhZ3k4blE6YTdlczhhNzQ4NUluakVPZg=="
}

response = requests.request('POST', url=access_token_url,headers=headers,data=body,verify=False)
response.text

$body = @{
    grant_type = "password"
    username = "chris.gilbert@bennetts.co.uk"
    password = "Trustpilotm1"
}

$headers = @{
        "Accept"="application/x-www-form-urlencoded";
        "Authorization"="MG1GZ2ZLQVpJR3YzWkd5YkUzelp2anNpMGZhZ3k4blE6YTdlczhhNzQ4NUluakVPZg=="
}

$access_response = Invoke-RestMethod -Method 'Post' -Uri $access_token_url -Body $body -Headers $headers

$access_token = $access_response.access_token


$reviews_url = "https://api.trustpilot.com/v1/private/business-units/56fa62070000ff00058ac3e6/reviews?perPage=100&access_token=" + "$access_token"


##Authentication
response = requests.request('POST', url, data=payload,verify=False)
token = json.loads(response.text)['sessionId']


headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/jsonV',
      'cookie' : "hazelcast.sessionId="+token
    }
