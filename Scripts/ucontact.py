#/usr/bin/python3
import requests
import json

def form_get(query = "", dsn = "Repo"):
    
    
    url = "http://localhost:8085/Integra/resources/forms/FormGet"
    payload='query={}&dsn={}'.format(query, dsn)
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        response = json.loads(response.text)
    except:
        print(response)
        print(response.text)
        response = []
        print("Error in the query, return [].")
        
    finally:
        print("Query executed")
        return response

def delete_data(table = "", dsn = "Repo"):
    url = "http://localhost:8085/Integra/resources/forms/FormExec"
    if table == "" or dsn == "": return {"error": "You most to fill all the params for this function"}
    query = "DELETE FROM {}".format(table)
    payload='query={}&dsn={}'.format(query, dsn)
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response
        
def insert_multiple_data(head = "",arrObj = [], table = "", db = "ccrepo", dsn="Repo"):
    query = ""
    vals = ""

    if table == "" or db == "": return {"error": "You most to fill all the params for this function"}
    query += "INSERT INTO {}.{} ({}) VALUES ".format(db, table, head)
    for ele in arrObj:
        s = ""
        for item in ele:
            s += "'" + str(ele[item]) + "'" + ","
        vals += "(" + s[:-1] + "),"
    vals = vals[:-1]
    query += vals
    
    url = "http://localhost:8085/Integra/resources/forms/FormExec"
    payload='query={}&dsn={}'.format(query, dsn)
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload)
    return response

def form_save(obj_str = "{\}", table = "", dsn = ""):
    url = "http://localhost:8085/Integra/resources/forms/FormSave"
    if obj_str == "{\}" or table == "" or dsn == "": return {"error": "You most to fill all the params for this function"}
    payload='json={}&datatype={}&dsn={}'.format(obj_str, table, dsn)
    # payload='json={}&datatype={}&dsn={}'.format('{"DisplayNumber": "000946935", "DateApplicationReceived": "2022-02-13 09:49:35.0"}', table, dsn) #TEST
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response