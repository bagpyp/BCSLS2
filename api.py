import requests

from config import api_key, client_id

base = "https://developer.sidelineswap.com/api/v1/"
headers = {"x-api-key": api_key, "x-client-id": client_id, "accept": "application/json"}


def get_category_by_id(id=""):
    res = requests.get(base + f"categories/{id}", headers=headers)
    return res


def post_listing(data):
    h = headers.copy()
    h.update({"content-type": "application/json"})
    res = requests.post(base + "listings", headers=h, json=data)
    return res


def set_address(data):
    # setAddress({'street_1': '2424 SE Burnside Rd',
    #     'city': 'Gresham',
    #     'state': 'OR',
    #     'zip': '97080',
    #     'country': 'US'
    # })
    h = headers.copy()
    h.update({"content-type": "application/json"})
    res = requests.post(base + "addresses", headers=h, json=data)
    return res
