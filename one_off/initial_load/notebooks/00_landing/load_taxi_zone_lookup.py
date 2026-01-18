# Databricks notebook source
import urllib.request
import os
import shutil

#Target url of public csv file to download
url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

#Open a connection to the remote URL and fetch file as a stream

response = urllib.request.urlopen(url)

#create a destination directory for storing the file

dir_path = "/Volumes/nyc_taxi/00_landing/data_sources/lookup"
os.makedirs(dir_path, exist_ok = True)

#Define the full local path (including filename) where file will be saved

local_path = f"{dir_path}/taxi_zone_lookup.csv"

#write the contents of the response stream to the specified local file path

with open(local_path,'wb') as f:
    shutil.copyfileobj(response,f)