#to unzip
import zipfile
with zipfile.ZipFile("grab_food_pages.zip","r")as z:
    z.extractall()

#load json from that files
nput_src_folder="folder_path"
def load_json(input_src_folder):
    json_data = []

    for name in os.listdir("D:/Siddharth/Grabfood_unzip_load/grab_food_pages"):
        if name.endswith(".gz"):

            full_path = os.path.join(input_src_folder, name)

            with gzip.open(full_path, "rt", encoding="utf-8") as f:
                json_data.append(json.load(f))

    return json_data

import os
import gzip
import json
from config import *

def load_files(data_dir,start_val,end_val):

    files = os.listdir(data_dir)
    files.sort()

    for file in files[start_val:end_val]:

        path = os.path.join(data_dir, file)

        try:

            with gzip.open(path, "rt", encoding="utf-8") as f:
                yield json.load(f)

        except Exception as e:

            print("Failed file:", file, e)
