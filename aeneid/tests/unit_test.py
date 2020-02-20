# Qi Feng (qf2140)

import requests
import json


def test_api():

    r = requests.get("http://127.0.0.1:5000")
    result = r.text

    print("First REST API returned", r.text)
    print("\n")


def test_simple_get_1():
    params = {"fields": "G_all, GS"}
    url = 'http://127.0.0.1:5000/api/lahman2017/appearances/willite01_1960_BOS'
    headers = {'Content-Type': 'application/json: charset=utf-8'}
    r = requests.get(url, headers=headers, params=params)
    print("Test for simple GET:")
    print("Result = ")
    print(r.text)
    print(json.dumps(r.json(), indent=2, default=str))
    print("\n")


def test_simple_get_2():
    try:
        params = {"fields": "G_all, GS, age"}
        url = 'http://127.0.0.1:5000/api/lahman2017/appearances/willite01_1960_BOS'
        headers = {'Content-Type': 'application/json: charset=utf-8'}
        r = requests.get(url, headers=headers, params=params)
        print("Test for simple GET (with undefined field):")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
    except Exception as e:
        print("Got exception e = ", e)
        print("\n")


def test_simple_get_3():
    try:
        params = {"nameLast": "Smith",
                  "fields": "playerID, nameLast, nameFirst",
                  "limit": 10,
                  "offset": 10,
                  "order_by": "nameFirst"}
        url = 'http://127.0.0.1:5000/api/lahman2017/people'
        headers = {'Content-Type': 'application/json: charset=utf-8'}
        r = requests.get(url, headers=headers, params=params)
        print("Test for simple GET (with limit, offset and order_by):")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
    except Exception as e:
        print("Got exception e = ", e)
        print("\n")


def test_related_get():
    try:
        params = {"fields": "ab,h",
                  "yearid": "1960"}
        url = 'http://127.0.0.1:5000/api/lahman2017/people/willite01/batting'
        headers = {'Content-Type': 'application/json: charset=utf-8'}
        r = requests.get(url, headers=headers, params=params)
        print("Test for related GET:")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
    except Exception as e:
        print("Got exception e = ", e)
        print("\n")


def test_simple_post():
    try:
        data = {
            "playerID": "dff1",
            "nameLast": "Ferguson",
            "nameFirst": "Donald"
        }
        url = 'http://127.0.0.1:5000/api/lahman2017/people'
        headers = {'Content-Type': 'application/json: charset=utf-8'}
        r = requests.post(url, headers=headers, json=data)
        print("Test for simple POST:")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
        print("\n")

        params = {"nameLast": "Ferguson",
                  "fields": "playerID,birthMonth,birthYear"}
        r = requests.get(url, headers=headers, params=params)
        print("Get the created entry:")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
        print("\n")

    except Exception as e:
        print("Got exception e = ", e)
        print("\n")


def test_simple_delete():
    try:
        url = 'http://127.0.0.1:5000/api/lahman2017/people/dff1'
        headers = {'Content-Type': 'application/json: charset=utf-8'}
        r = requests.delete(url, headers=headers)
        print("Test for simple DELETE:")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
        print("\n")

        params = {"nameLast": "Ferguson",
                  "fields": "playerID,birthMonth,birthYear"}
        r = requests.get(url, headers=headers, params=params)
        print("Check whether the entry has been deleted:")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
        print("\n")

        url = 'http://127.0.0.1:5000/api/lahman2017/people/dff1'
        r = requests.delete(url, headers=headers)
        print("Test for simple DELETE (if entry DNE):")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
        print("\n")

    except Exception as e:
        print("Got exception e = ", e)
        print("\n")


def test_related_post():
    try:
        data = {
            "yearID": "2020",
            "teamID": "BOS",
            "stint": "1",
            "H": "1000",
            "AB": "1000"
        }
        url = 'http://127.0.0.1:5000/api/lahman2017/people/willite01/batting'
        headers = {'Content-Type': 'application/json: charset=utf-8'}
        r = requests.post(url, headers=headers, json=data)
        print("Test for related POST:")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
        print("\n")

        params = {"H": "1000",
                  "fields": "yearID,teamID,playerID,stint,AB"}
        r = requests.get(url, headers=headers, params=params)
        print("Get the created entry:")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
        print("\n")

    except Exception as e:
        print("Got exception e = ", e)
        print("\n")


def test_simple_update():
    try:
        data = {
            "playerID": "dff1",
            "nameLast": "Ferguson",
            "nameFirst": "Donald"
        }
        url = 'http://127.0.0.1:5000/api/lahman2017/people'
        headers = {'Content-Type': 'application/json: charset=utf-8'}
        r = requests.post(url, headers=headers, json=data)
        print("Create entry for simple UPDATE:")
        print("Result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
        print("\n")

        data = {
            "birthYear": "1960",
            "birthMonth": "1"
        }
        r = requests.put(url, headers=headers, json=data)
        print("After update result = ")
        print(r.text)
        print(json.dumps(r.json(), indent=2, default=str))
        print("\n")

    except Exception as e:
        print("Got exception e = ", e)
        print("\n")

test_api()
test_simple_get_1()
test_simple_get_2()
test_simple_get_3()
test_related_get()
test_simple_post()
test_simple_delete()
test_related_post()
test_simple_update()
