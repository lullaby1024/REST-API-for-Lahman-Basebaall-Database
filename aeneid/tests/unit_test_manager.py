# Qi Feng (qf2140)
import requests
import json


def display_response(rsp):

    try:
        print("Printing a response.")
        print("HTTP status code: ", rsp.status_code)
        h = dict(rsp.headers)
        print("Response headers: \n", json.dumps(h, indent=2, default=str))

        try:
            body = rsp.json()
            print("JSON body: \n", json.dumps(body, indent=2, default=str))
        except Exception as e:
            body = rsp.text
            print("Text body: \n", body)

    except Exception as e:
        print("display_response got exception e = ", e)


def test_create_manager():

    try:
        body = {
            "id": "ok1",
            "last_name": "Obiwan",
            "first_name": "Kenobi",
            "email": "ow@jedi.org"
        }
        print("\ntest_create_manager: test 1, manager = \,", json.dumps(body, indent=2, default=str))
        url = "http://127.0.0.1:5000/api/moneyball/fantasy_manager"
        headers = {"content-type": "application/json"}
        result = requests.post(url, headers=headers, json=body)
        display_response(result)

        print("\ntest_create_manager: test 2 retrieving created manager.")
        link = result.headers.get('Location', None)
        if link is None:
            print("No link header returned.")
        else:
            url = link
            headers = None
            result = requests.get(url)
            print("\ntest_create_manager: Get returned: ")
            display_response(result)

        print("\ntest_create_manager: test 1, creating duplicate = \,", json.dumps(body, indent=2, default=str))
        url = "http://127.0.0.1:5000/api/moneyball/fantasy_manager"
        headers = {"content-type": "application/json"}
        result = requests.post(url, headers=headers, json=body)
        display_response(result)

    except Exception as e:
        print("POST got exception = ", e)


def test_update_manager():

    try:

        print("\ntest_update_manager: test 1, get manager with id = ls1")
        url = "http://127.0.0.1:5000/api/moneyball/fantasy_manager/ls1"
        result = requests.get(url)
        print("test_update_manager: get result = ")
        display_response(result)

        body = {
            "last_name": "Darth",
            "first_name": "Vader",
            "email": "dv@deathstar.navy.mil"
        }

        print("\ntest_update_manager: test 2, updating data with new value = ")
        print(json.dumps(body))
        headers = {"Content-Type": "application/json"}
        result = requests.put(url, headers=headers, json=body)
        print("\ntest_update_manager: test 2, updating response =  ")
        display_response(result)

        print("\ntest_update_manager: test 3, get manager with id = ls1")
        url = "http://127.0.0.1:5000/api/moneyball/fantasy_manager/ls1"
        result = requests.get(url)
        print("test_update_manager: get result = ")
        display_response(result)

    except Exception as e:
        print("PUT got exception = ", e)


def test_get_by_path():

    try:

        team_id=27
        sub_resource = "fantasy_manager"

        print("\ntest_get_by_path: test 1")
        print("team_id = ", team_id)
        print("sub_resource = ", sub_resource)

        path_url = "http://127.0.0.1:5000/api/moneyball/fantasy_team/" + str(team_id) + "/" + sub_resource
        print("Path = ", path_url)
        result = requests.get(path_url)
        print("test_get_by_path: path_url = ")
        display_response(result)

    except Exception as e:
        print("PUT got exception = ", e)


def test_create_related():

    try:

        playerid = 'ls1'
        sub_resource = "fantasy_team"

        print("\ntest_create_related: test 1")
        path_url = "http://127.0.0.1:5000/api/moneyball/fantasy_manager/ls1/fantasy_team"
        print("Path = ", path_url)
        body = {"team_name": "Braves"}
        print("Body = \n", json.dumps(body, indent=2))
        result = requests.post(path_url, json=body, headers={"Content-Type" : "application/json"})
        print("test_create_related response = ")
        display_response(result)

        l = result.headers.get("Location", None)
        if l is not None:
            print("Got location = ", l)
            print("test_create_related, getting new resource")
            result = requests.get(l)
            display_response(result)
        else:
            print("No location?")

    except Exception as e:
        print("POST got exception = ", e)

# test_create_manager()
# test_update_manager()
# test_get_by_path()
# test_create_related()