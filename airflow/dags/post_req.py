# importing the requests library
import requests
import json 


task=json.dumps({"sql":"SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production a, colors_production b WHERE a.color_name = b.color_name LIMIT 5;"})

header = {"Content-Type" : "application/json", "Authorization" : "Bearer eyJ4NXUiOiJpbXMta2V5LTEuY2VyIiwiYWxnIjoiUlMyNTYifQ.eyJpZCI6IjE1MTY2OTY4Mzc2NDdfZmNmNGVmYWItZGVmZi00YWQxLTllNjQtZjYwOGQzZDJhYWUyX3VlMSIsImNsaWVudF9pZCI6Ik1DRFBDYXRhbG9nU2VydmljZVFhMiIsInVzZXJfaWQiOiJNQ0RQQ2F0YWxvZ1NlcnZpY2VRYTJAQWRvYmVJRCIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJhcyI6Imltcy1uYTEtcWEyIiwicGFjIjoiTUNEUENhdGFsb2dTZXJ2aWNlUWEyX2RldnFhIiwicnRpZCI6IjE1MTY2OTY4Mzc2NDhfMjU2MzY1NWMtNmM4YS00YTA3LWI2YWItZTIzMmZjNTA3ZWIwX3VlMSIsInJ0ZWEiOiIxNTE3OTA2NDM3NjQ4IiwibW9pIjoiNzMyYTM1ZDgiLCJjIjoiTmxUd0dsY1l3cFh2eE4vWktNRUJGQT09IiwiZXhwaXJlc19pbiI6Ijg2NDAwMDAwIiwic2NvcGUiOiJzeXN0ZW0sQWRvYmVJRCxvcGVuaWQiLCJjcmVhdGVkX2F0IjoiMTUxNjY5NjgzNzY0NyJ9.D7Pq8EJO5qACX0SWpRKtRslKeaB9HwNobh5HFPMV4Z0xuKfrkpIex2Xom5OUdGDb0B4Jd4sINFMIogKsHkFVgXOLb8usVnnAn5JjtlE_5TeOxfkPub4VUG-_bTKR1gLpcJwOzs59-foa0VIpD13z0VXYixRhn6l0kMGZYhk_c6jj90l6wjGKXTxcq9DUO2cr_uZnog0K340JK8Y7FVpA-d4FYx7IN4CHs3567DvRj1O12hNhujValNO3-_K7NZquuPYlUUcmZ3EHDSViRrSjUtoRVJQM6QBgcO64mrYunNJtPZ_TE4URDeXDPvOK2SQPkp3UEcDVvwT3tnQVlfKPLw"}

resp = requests.post('http://prod.xyz.com:1234/query/12345@XyOrg/test',
                     data=task,
                     headers=header)
                     
# extracting response text 
Final_Response = resp.text
print("The pastebin URL is:%s"%Final_Response)                     