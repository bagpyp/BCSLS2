# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 14:14:12 2020

@author: Bagpyp
"""


hoursAgo = 24 * 10


import datetime as dt

beginning = dt.datetime.now()
print("began", beginning.ctime())
from html2text import html2text

# import json
from numpy import nan, where
import pandas as pd

pd.options.mode.chained_assignment = None
pd.options.display.max_rows = 150
pd.options.display.max_columns = 40
pd.options.display.width = 180
pd.options.display.max_colwidth = 40

import requests
import sys

# TO TEST
# uncomment the following line and point the program at one product
# sys.argv.append('<insert webName here (name as in BC)>')
# for example
# sys.argv.append('State 2.0 W/Remix Boot 2020')

if len(sys.argv) > 1:
    drive = "T"
    hoursAgo = 24 * 10 * 365


def log(text, stamp=False, base="log"):
    with open(f"logs/{base} {dt.datetime.now().strftime('%Y-%m-%d')}.txt", "a") as file:
        if stamp:
            file.write(f"\n\n\n{dt.datetime.now().strftime('%H:%M:%S')}\n\n")
        file.write(text)
        file.write("\n")


with open("apiKey.txt") as file:
    a = file.readline()
    b = file.readline()

base = "https://developer.sidelineswap.com/api/v1/"
headers = {"x-api-key": a[:-1], "x-client-id": b, "accept": "application/json"}


def getCat(id=""):
    res = requests.get(base + f"categories/{id}", headers=headers)
    return res


def postList(data):
    h = headers.copy()
    h.update({"content-type": "application/json"})
    res = requests.post(base + "listings", headers=h, json=data)
    return res


def setAddress(data):
    h = headers.copy()
    h.update({"content-type": "application/json"})
    res = requests.post(base + "addresses", headers=h, json=data)
    return res


# res = setAddress({'street_1': '2424 SE Burnside Rd',
#   'city': 'Gresham',
#   'state': 'OR',
#   'zip': '97080',
#   'country': 'US'})

# if res.ok:
#     print('address set')
cmap = pd.read_csv("cat_map.csv")
det = pd.read_csv("detail_map.csv")

for df in [cmap, det]:
    df.dcsname = df.dcsname.str.replace("hike/pack", "hike, pack")

cmap = cmap[cmap.uuid.notna()]
# remove tabs
for c in ["uuid", "cat"]:
    cmap[c] = cmap[c].str.replace("\t", "")
det = det[det.dcsname.isin(cmap.dcsname)]

c = cmap.set_index("dcsname").cat.to_dict()

det["dic"] = det.detail.fillna("") + ":" + det.field.fillna("")

# comment out BEGIN to test locally


df = (
    pd.read_pickle(r"..\RPBC2\ready.pkl")
    .reset_index()
    .replace({nan: None})
    .drop(["fCreated", "p_date_created", "p_date_modified"], axis=1)
)


"""

SYS ARG V

"""


if len(sys.argv) > 1:
    df = df.loc[df.webName.str.lower().isin([s.lower() for s in sys.argv[1:]]), :]
    df = df.loc[df.sku.notna(), :]
else:
    df = df.loc[df.sku.notna(), :]

# filter by mask passed by cmd


df["dcsname"] = df.DCSname.str.replace("Hike/Pack", "Hike, Pack").str.lower()
df = df[df.dcsname.isin(list(c.keys()))]
df[list("dcs")] = df.dcsname.str.split("/", expand=True)
df["cond"] = where(df.DCSname.str.contains("clearance"), "used", "new")
# not NEw, the condition
df.webName = where(
    df.webName.str.split(" ").apply(lambda x: len(x)) == 2,
    "New: " + df.webName,
    df.webName,
)

df.webName = where(
    df.webName.str.split(" ").apply(lambda x: len(x)) == 1,
    " from Hillcrest" + df.webName,
    df.webName,
)
# has images only
df = df.groupby("webName").filter(
    lambda g: ((g.image_0.count() > 0) & (g.qty.sum() > 0))
    | (g.lModified.max() > dt.datetime.now() - dt.timedelta(hours=hoursAgo))
)

# strip html
df.description = df.description.apply(lambda x: html2text(x) if x else x)

# bad prices
df = df[df.pSale >= 3]
df.pMSRP = df.pMSRP.fillna(df.pSale)

# comment out END to test locally
df.to_pickle("currentDf.pkl")

# comment out BEGIN to run
# df = pd.read_pickle('currentDf.pkl')
# comment our END to run


req = pd.read_csv("category_fields.csv")

req = req[(req.field.str.lower() != "brand") & (req.field.str.lower() != "condition")]

notreq = req[req.required == 0]
notreq = notreq[["catname", "field"]]
opFields = {
    n: f.field.unique().tolist() for n, f in notreq.groupby("catname", sort=False)
}

req = req[req.required == 1]
req = req[["catname", "field"]]
fields = {n: f.field.unique().tolist() for n, f in req.groupby("catname", sort=False)}

# print(req.head())

db = det[["dcsname", "dic"]].groupby("dcsname", sort=False)
# ds does not contain 'safety/helmet/ski' as key - fixed
# ds does not contain 'rental/ski/binding' as key....

ds = {}
for n, d in db:
    dl = d.dic.values.tolist()
    kvs = {s.split(":")[0]: s.split(":")[1] for s in dl}
    kvs.update({"Condition": "cond"})
    if "" in kvs:
        kvs.pop("")
    ds.update({n: kvs})
for k in list(c.keys()):
    if k not in ds:
        ds.update({k: {"Condition": "cond"}})

# apostaphe's
for s in list("dcs"):
    df[s] = df[s].str.replace("^mens$", "Men's", regex=True)
    df[s] = df[s].str.replace("^womens$", "Women's", regex=True)
    df[s] = df[s].str.replace("^men$", "Men's", regex=True)
    df[s] = df[s].str.replace("^women$", "Women's", regex=True)

# titles
df["title"] = (
    df.webName + " " + df["size"].fillna("") + " " + df.color.fillna("")
).str.replace(" +", " ", regex=True)

# prices
for col in ["pSale", "pMSRP"]:
    df[col] = df[col].astype(float).round(2)

# %%

# df = df[df.CAT.str.contains('Watersport')]

gb = df.groupby("webName", sort=False)
failures = []
reqs = []
N = gb.ngroups
i = 0
for i, (n, g) in enumerate(gb):
    i += 1
    print(f"{i}/{N}")
    g = g.to_dict("records")
    g0 = g[0]
    if len(g) > 1:
        g = g[1:]
    else:
        g = [g0.copy()]
    cat = c[g0["dcsname"]]
    data = {
        "listing_sku": g0["sku"],
        "name": g0["webName"],
        "description": g0["description"],
        "category": cat,
        "brand": g0["BRAND"],
        "model": g0["name"],
        "accepts_offers": True,
        "ship_from_address_id": "db4f7b6a-91d8-4fbb-806a-d07e7449f5e2",
        "images": [g0[f"image_{i}"] for i in range(5) if g0[f"image_{i}"]],
        "items": [],
    }
    for h in g:
        item = {
            "name": h["title"],
            "item_sku": h["sku"],
            "quantity": h["qty"],
            "list_price": h["pSale"],
            "retail_price": h["pMSRP"],
            "gtin": h["UPC"],
            "mpn": h["mpn"],
            "images": [h["v_image_url"]],
            "details": [
                {"type": k, "option": h[v]} for k, v in ds[h["dcsname"]].items()
            ],
        }
        if cat in fields:
            want = fields[cat]
        else:
            want = []
        if cat in opFields and "Color" in opFields[cat]:
            item["details"].extend([{"type": "Color", "option": h["color"]}])
        have = [detail["type"] for detail in item["details"]]
        need = [x for x in want if x not in have]
        # if Size ends up in need, just grab if from h!!!
        item["details"].extend([{"type": n, "option": "other"} for n in need])
        data["items"].append(item)
    res = postList(data)
    if res.status_code != 200:
        failures.append(res)
    else:
        reqs.append(res)

    #
    # log("FAILURES", stamp=True, base='ALLfailureLog')
    # for i,f in enumerate(failures):
    #     logtext = '\n'+'-'*100+f'\n\n{i}\n\n' \
    #         + f.text \
    #         + '\n\n'
    #         # + json.dumps(json.loads(f.request.body)) \
    #         # + '\n\n'
    #     log(logtext, base='ALLfailureLog')
    #
    # print(f'runtime: {(dt.datetime.now()-beginning).seconds} seconds, bitch!')
    #
    # if len(sys.argv) > 0:
    #     from pprint import pprint
    #     for r in reqs:
    #         print(r.json())
    #     for f in failures:
    #         pprint(f.json())

# %%

# for h in data['items']:
#     print('original details',h['details'])
#     want = fields[data['category']]
#     print('want: ', want)
#     have = [detail['type'] for detail in h['details']]
#     print('\thave: ',have)
#     need = [x for x in want if x not in have]
#     print('\t\tneed: ',need)
#     h['details'].extend([{'type':n,'option':'other'} for n in need])
#     print('fixed details',h['details'],'\n')

# %%

# for r in reqs:
#     print('CALL MADE\n')
#     pprint(json.loads(r.request.body))
#     print('\n', 'RESPONSE')
#     pprint(json.loads(r.content))
#     print('\n'+'-'*100+'\n')
