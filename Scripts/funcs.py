#/usr/bin/python3
import json
import ucontact as uc
from datetime import date, timedelta

def insert_jsonfile(labels = [], datasets = [], screen = "", propertyname = "", filename = "data.json"):
    _content = ""

    with open(filename, "r+") as f:
        _content = f.readlines()
        _content = json.loads(_content[0])
        _content[screen][propertyname]["datasets"] = datasets
        _content[screen][propertyname]["labels"] = labels

    with open(filename, 'w') as f:
        json.dump(_content, f)

    return "todo ok"

def get_date_range(range):
    
    today = date.today()
    day = today - timedelta(days=range)
    day = day.isoformat()
    today = today.isoformat()
    return {"today": today, "day": day}

def push_data_to_dashboard(query = "", dsn = "Repo", lbl = "label_field_name", dst = "dataset_field_name", filename = "data.json", screen = "screen1", prop = "property_name"):
    response_get = uc.form_get(query, dsn)
    datasets = []
    labels = []
    for ele in response_get:
        datasets.append(ele[dst])
        labels.append(ele[lbl])

    insert_jsonfile(labels, datasets, screen, prop, filename)