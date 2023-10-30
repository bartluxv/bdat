import requests
import json

url = 'http://127.0.1:5000/api/tohive'
data = {
    "table": "tmp",
    "data": [{"clint_id": 1, "ds": '2'}
             ,{"clint_id": '1', "ds": '2'}]

}
res=requests.post(url, json=data)
print(res.text)
