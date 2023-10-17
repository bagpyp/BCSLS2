# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 14:14:12 2020

@author: Bagpyp
"""
import datetime as dt

import pandas as pd
from html2text import html2text
from numpy import nan, where

from api import post_listing
from config import update_window_hours, address_id

df = (
    pd.read_pickle(r"..\RPBC2\data\ready.pkl")
    .reset_index()
    .replace({nan: None})
    .drop(["fCreated", "p_date_created", "p_date_modified"], axis=1)
)
df["dcsname"] = df.DCSname.str.replace("Hike/Pack", "Hike, Pack").str.lower()

category_map = pd.read_csv("category_map.csv")
category_map = category_map[category_map.uuid.notna()]
dcs_name_to_sls_cat = category_map.set_index("dcsname").cat.to_dict()
df = df[df.dcsname.isin(list(dcs_name_to_sls_cat.keys()))]


# apostrophes
df[list("dcs")] = df.dcsname.str.split("/", expand=True)
for s in list("dcs"):
    df[s] = df[s].str.replace("^mens$", "Men's", regex=True)
    df[s] = df[s].str.replace("^womens$", "Women's", regex=True)
    df[s] = df[s].str.replace("^men$", "Men's", regex=True)
    df[s] = df[s].str.replace("^women$", "Women's", regex=True)

# titles
df["title"] = (
    (df.webName + " " + df["size"].fillna("") + " " + df.color.fillna(""))
    .str.replace(" +", " ", regex=True)
    .str.strip()
)

# prices
for col in ["pSale", "pMSRP"]:
    df[col] = df[col].astype(float).round(2)

# condition
df["cond"] = where(df.dcsname.str.contains("clearance"), "used", "new")

# not new, the condition TODO: wtf?
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
    | (g.lModified.max() > dt.datetime.now() - dt.timedelta(hours=update_window_hours))
)

# strip html
df.description = df.description.apply(lambda x: html2text(x) if x else x)

# bad prices
df = df[df.pSale >= 3]
df.pMSRP = df.pMSRP.fillna(df.pSale)


category_field_requirements = pd.read_csv("category_field_requirements.csv")
category_field_requirements = category_field_requirements[
    (category_field_requirements.field.str.lower() != "brand")
    & (category_field_requirements.field.str.lower() != "condition")
]

optional_fields = category_field_requirements[category_field_requirements.required == 0]
optional_fields = optional_fields[["catname", "field"]]
op_fields = {
    n: f.field.unique().tolist()
    for n, f in optional_fields.groupby("catname", sort=False)
}

category_field_requirements = category_field_requirements[
    category_field_requirements.required == 1
]
category_field_requirements = category_field_requirements[["catname", "field"]]
req_fields = {
    n: f.field.unique().tolist()
    for n, f in category_field_requirements.groupby("catname", sort=False)
}


detail_map = pd.read_csv("detail_map.csv")
detail_map = detail_map[detail_map.dcsname.isin(category_map.dcsname)]
detail_map["accessors_by_field"] = (
    detail_map.detail.fillna("") + ":" + detail_map.field.fillna("")
)
field_accesors_by_dcs_name = detail_map[["dcsname", "accessors_by_field"]].groupby(
    "dcsname", sort=False
)

accessors = {}
for dcs_name, field_accessor_series in field_accesors_by_dcs_name:
    dl = field_accessor_series.accessors_by_field.values.tolist()
    accessor_dict = {s.split(":")[0]: s.split(":")[1] for s in dl}
    accessor_dict.update({"Condition": "cond"})
    if "" in accessor_dict:
        accessor_dict.pop("")
    accessors.update({dcs_name: accessor_dict})
for dcs_name in list(dcs_name_to_sls_cat.keys()):
    if dcs_name not in accessors:
        accessors.update({dcs_name: {"Condition": "cond"}})

df.to_pickle("ready.pkl")

gb = df.groupby("webName", sort=False)
failures = []
successes = []
for _, g in gb:
    g = g.to_dict("records")
    g0 = g[0]
    if len(g) > 1:
        g = g[1:]
    else:
        g = [g0.copy()]
    cat = dcs_name_to_sls_cat[g0["dcsname"]]
    data = {
        "listing_sku": g0["sku"],
        "name": g0["webName"],
        "description": g0["description"],
        "category": cat,
        "brand": g0["BRAND"],
        "model": g0["name"],
        "accepts_offers": True,
        "ship_from_address_id": address_id,
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
                {"type": k, "option": h[v]} for k, v in accessors[h["dcsname"]].items()
            ],
        }
        if cat in req_fields:
            want = req_fields[cat]
        else:
            want = []
        if cat in op_fields and "Color" in op_fields[cat]:
            item["details"].extend([{"type": "Color", "option": h["color"]}])
        have = [detail["type"] for detail in item["details"]]
        need = [x for x in want if x not in have]
        # if Size ends up in need, just grab if from h!!!
        item["details"].extend([{"type": n, "option": "other"} for n in need])
        data["items"].append(item)
    res = post_listing(data)
    if res.status_code != 200:
        failures.append(res)
    else:
        successes.append(res)

print(len(failures), "failures")
print(len(successes), "successes")
