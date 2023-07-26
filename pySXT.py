# -*- coding: utf-8 -*-
import requests
import sys, os
import json, logging, datetime
from nacl.signing import SigningKey
import base64


class sxt():

    standard_headers = {"accept": "application/json",  "content-type": "application/json"}
    api_url          = os.environ.get('API_URL')
    userid          = os.environ.get('USERID')
    user_private_key = os.environ.get('USER_PRIVATE_KEY')
    user_public_key  = os.environ.get('USER_PUBLIC_KEY')
    app_name         = os.environ.get('APP_NAME')
    access_token     = os.environ.get('TOKEN')
    refresh_token    = os.environ.get('REFRESH_TOKEN')
    api_version      = 'v1'
    reauth_minutes   = 20
    reauth_datetime  = datetime.datetime.now() + datetime.timedelta(seconds=-1)

    def __init__(self, envfile=None, api_url=None, userid=None, user_private_key=None, user_public_key=None, app_name=None, api_version=None) -> None:
        if envfile: 
            with open(file=envfile, mode="r") as fh:
                for line in fh.readlines():
                    ary=line.split("=") 
                    n=ary[0].strip().lower().replace('"',"")
                    v=ary[1].strip().replace('"',"")
                    if n=="api_url" and v[-1:]!='/': v=v+'/'
                    if hasattr(self, n): setattr(self, n, v)

        self.api_version      = api_version if api_version else self.api_version
        self.api_url          = api_url if api_url else self.api_url + self.api_version + '/'
        self.userid          = userid if userid else self.userid
        self.user_private_key = user_private_key if user_private_key else self.user_private_key
        if self.user_private_key[-1:] != '=': self.user_private_key = self.user_private_key + '='
        self.user_public_key  = user_public_key if user_public_key else self.user_public_key
        if self.user_public_key[-1:] != '=': self.user_public_key = self.user_public_key + '='
        self.app_name         = app_name if app_name else self.app_name
        

        
        

    # https://docs.spaceandtime.io/reference/authentication-code
    def authenticate(self):
        try:
            auth_code = self.request_auth_code()
            signed_auth_code = self.sign_message(auth_code)
            self.access_token, self.refresh_token = self.request_token(auth_code, signed_auth_code)
            self.reauth_datetime = datetime.datetime.now() + datetime.timedelta(minutes=self.reauth_minutes)
            return True, self.access_token, self.refresh_token, self.reauth_datetime
        except Exception as ex: 
            self.reauth_datetime = datetime.datetime.now() + datetime.timedelta(seconds=-1)
            return False, str(ex), "", self.reauth_datetime
        
    def reauth_soon(self):
        return self.reauth_datetime < datetime.datetime.now()

    # https://docs.spaceandtime.io/reference/authentication-code
    def request_auth_code(self):
        url = self.api_url + "auth/code"
        payload = { "userId": self.userid }
        resp = requests.post(url, json=payload, headers=self.standard_headers)
        jsonResponse = resp.json()

        if resp.status_code == 200: 
            auth_code = jsonResponse["authCode"]
        else: 
            print('Non 200 response from the auth/code endpoint!')
            auth_code = None

        return auth_code 


    def sign_message(self, auth_code):
        # get bytes of the auth code for signing  
        bytes_message = bytes(auth_code, 'utf-8')
        # decode private key for signing 
        key = base64.b64decode(self.user_private_key)
        # create signing key
        signingkey = SigningKey(key)
        # finally, sign the auth code with our private key
        signed_message = signingkey.sign(bytes_message)

        return signed_message[:64].hex()


    # https://docs.spaceandtime.io/reference/token-request
    def request_token(self, auth_code, signed_auth_code):
        url = self.api_url + "auth/token"
        payload = {
            "userId": self.userid,
            "authCode": auth_code,
            "signature": signed_auth_code,
            "key": self.user_public_key
        }
        resp = requests.post(url, json=payload, headers=self.standard_headers)    
        jsonResp = resp.json()

        return jsonResp["accessToken"],jsonResp["refreshToken"]



    # https://docs.spaceandtime.io/reference/execute-queries-dql
    def query_dql(self, accesstoken="", resourceId="", sql="", biscuit = None, rowcount = 1000):
        return self.query('dql', accesstoken, resourceId, sql, biscuit, rowcount)

    # https://docs.spaceandtime.io/reference/modify-data-dml
    def query_dml(self, accesstoken="", resourceId="", sql="", biscuit = None):
        return self.query('dml', accesstoken, resourceId, sql, biscuit, None)

    # https://docs.spaceandtime.io/reference/configure-resources-ddl
    def query_ddl(self, accesstoken="", resourceId="", sql="", biscuit = None):
        return self.query('ddl', accesstoken, resourceId, sql, biscuit, None)


    def query(self, type, accesstoken="", resourceId="", sql="", biscuit = None, rowcount = None):
        url = self.api_url + "sql/" + type
        if resourceId == "": return -1, {"must supply a resourceId (typically schema.tablename)"}
        if sql == "": return -1, {"must supply sql to execute"}
        if accesstoken =="": accesstoken = self.access_token
        if accesstoken =="": return -1, {"must supply a valid Access_Token (try running 'authenticate()' first)"}

        header = self.standard_headers.copy()
        header['authorization'] = f"Bearer {accesstoken}"
        if biscuit: header['biscuit'] = biscuit
        if self.app_name: header['originApp'] = self.app_name
        payload = {
            "resourceId": resourceId,
            "sqlText": sql,
        }
        if rowcount: payload["rowCount"] = rowcount
        try:
            resp = requests.post(url, json=payload, headers=header)    
        except Exception as ex: 
            return -1, {"Error returning 'requests' object (possible malformed request, check python process)\n":ex}
        try:
            jsonResp = resp.json()
            return resp.status_code, jsonResp 
        except Exception as ex: 
            return resp.status_code, {"reason": resp.reason, "error":ex}





 
