# ListenField Test Task
# Create API to return the image, temperature, and precipitation from the satellite
# Developed by: Nontawit Markjan

####################################################################################
### Import
####################################################################################
import os
import ee
import json
import requests
import pandas as pd

from datetime import datetime
from google.cloud import bigquery
from shapely.geometry import Polygon

from fastapi import FastAPI

dir_path = os.path.dirname(os.path.realpath(__file__))
credentials_json_path = dir_path + "\listenfield-test-task-svc-account.json"

# BigQuery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_json_path

# Earth Engine
service_account = "de-test-task@listenfield-satellite.iam.gserviceaccount.com"
credentials = ee.ServiceAccountCredentials(service_account, credentials_json_path)
ee.Initialize(credentials)

client = bigquery.Client()

# Fast API
app = FastAPI()

# # Under construction
# @app.post('/api/getData')
# def create_cities(request): # same body as 'City' class
#     request = json.load(request)
#     print(request["date_start"])


####################################################################################
### Temperature, Precipitation, ...
####################################################################################
# request parameters from user
# sample
lon_lat = [
    [100.92425737949549, 12.744458546688868],
    [101.52575884433924, 12.763210266433301],
    [101.51477251621424, 13.54683370428229],
    [100.98292193027674, 13.368701842404277],
]  # CHON BURI
#lon_lat = []
lon_lat_where_condition = ""
date_start = datetime.strptime("20160318", "%Y%m%d").date()
date_end = datetime.strptime("20160408", "%Y%m%d").date()

# response to user
status = ""
data_csv = {
    "Date": [],
    "Latitude": [],
    "Longitude": [],
    "Temperature": [],
    "Precipitation": [],
}

# Temperature and Precipitation
# DONE: query multiple lat, long
#     DONE  - find nearest lat, long if not found using WILDCARD and some algorithm
#     DONE  - find centroid
# DONE: query date in the range (start, end)
# DONE: response if no data available
# DONE: combine data to .csv
# can't query multiple years

# append centroid of the polygon to the list 'lon_lat'
centroid = list(Polygon(lon_lat).centroid.coords)
lon_lat.append(list(centroid[0]))

for i in lon_lat:
    # truncate lon, lat to 2 decimals so that the query won't be too specific
    lon_1_decimal = (str(i[0]).split(".")[0] + "." + str(i[0]).split(".")[1][0])  # example result : 100.92425737949549 -> 100.9
    lat_1_decimal = (str(i[1]).split(".")[0] + "." + str(i[1]).split(".")[1][0])  # example result : 12.744458546688868 -> 12.7

    # Create WHERE statement depends on the size of polygon (lat,lon) request using regex
    # example result :
    # (REGEXP_CONTAINS(cast(lon AS STRING),r'^101.3') AND REGEXP_CONTAINS(cast(lat AS STRING),r'^14.8')) OR
    # (REGEXP_CONTAINS(cast(lon AS STRING),r'^99.8') AND REGEXP_CONTAINS(cast(lat AS STRING),r'^19.88')) OR ...
    lon_lat_where_condition += ("(REGEXP_CONTAINS(cast(lon AS STRING),r'^"
        + lon_1_decimal + "') AND REGEXP_CONTAINS(cast(lat AS STRING),r'^"
        + lat_1_decimal+ "'))")
    if lon_lat.index(i) != len(lon_lat) - 1:
        lon_lat_where_condition += " OR "

# Because gsod dataset before 2020 don't have the proper 'date' feature,
# query the data with no specific date first, then transform the dataset later
# and do another query with clean dataset (with 'date' feature) is the solution
query = ("""
#standardSQL
SELECT      temp, prcp, name, da, mo, year, lat, lon
FROM        `bigquery-public-data.noaa_gsod.gsod""" + str(date_start.year) + """` a
LEFT JOIN   `bigquery-public-data.noaa_gsod.stations` b
ON a.stn=b.usaf AND a.wban=b.wban
WHERE """ + lon_lat_where_condition)

query_job = client.query(query)  # Make an API request.
query_result = query_job.result()

if query_result.total_rows == 0:
    status = "No data available"
else:
    status = str(query_result.total_rows) + " record(s) are founded"

    # row[0] = temperature (Kelvin)
    # row[1] = precipitation
    # row[2] = province
    # row[3] = day
    # row[4] = month
    # row[5] = year
    # row[6] = longitude
    # row[7] = latitude
    # row[8] = _DATE
    print(status + ":")
    for row in query_job:
        row = list(row)
        _DATE = datetime.strptime(
            str(row[5] + row[4] + row[3]), "%Y%m%d"
        ).date()  # ex. 2020-10-23
        row.append(_DATE)  # row[6]

        if row[8] > date_start and row[8] <= date_end:
            print(row)
            # Export to .csv
            data_csv["Date"].append(row[8])
            data_csv["Latitude"].append(row[7])
            data_csv["Longitude"].append(row[6])
            data_csv["Temperature"].append(row[0])
            data_csv["Precipitation"].append(row[1])

    df = pd.DataFrame(
        data_csv,
        columns=["Date", "Latitude", "Longitude", "Temperature", "Precipitation"],
    )
    df.to_csv(dir_path + "\export.csv", index = False, header = True)

####################################################################################
### True color satellite images
####################################################################################
# DONE: crop the image to fit the polygon
# TODO: export file as GeoTIFF
# # This code below can export the file
# landsat = ee.Image('LANDSAT/LC08/C01/T1_TOA/LC08_123032_20140515')\
#             .select(['B4', 'B3', 'B2'])

# geometry = ee.Geometry.Rectangle([116.2621, 39.8412, 116.4849, 40.01236])

# task = ee.batch.Export.image.toDrive(**{
#     'image': landsat,
#     'description': 'Once I can export the GeoTIFF',
#     'folder':'ListenField_Test_Task',
#     'scale': 100,
#     'region': geometry.getInfo()['coordinates']
# })
# task.start()

area = ee.Geometry.Polygon([lon_lat])
collection = (
    ee.ImageCollection("LANDSAT/LC08/C01/T1_RT_TOA")
    .filterDate(date_start, date_end)
    .filterBounds(area)
)

croppedRegion = collection.median()
img_exported = (
    croppedRegion.clip(area).select(["B4", "B3", "B2"]).visualize(min=0.0, max=0.3)
)

task = ee.batch.Export.image.toDrive(
    **{
        "image": img_exported,
        "folder": "ListenField_Test_Task", # shared folder in Google Drive
        "description": "Holy GeoTIFF",
        "scale": 30,
        "fileFormat": "GeoTIFF",
        "region": img_exported.getInfo()["coordinates"],
    }
)

task.start()
print(task.status())

# True color satellite images
# TODO: export to somewhere and try to pull back to server

# return "200"