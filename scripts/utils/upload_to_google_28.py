# %%
import pathlib
import sys
from importlib.resources import path
import os

# make modules importable when running this file as script
# sys.path.append(str(pathlib.Path(__file__).parent.parent))

import geojson
import xarray as xr
from coclicodata.etl.cloud_utils import (
    p_drive,
    dir_to_google_cloud,
    dataset_to_google_cloud,
    dataset_from_google_cloud,
    geojson_to_mapbox,
    load_env_variables,
    load_google_credentials,
)
from coclicodata.etl.extract import (
    clear_zarr_information,
    get_geojson,
    get_mapbox_url,
    zero_terminated_bytes_as_str,
)
from coclicodata.coclico_stac.utils import (
    get_dimension_dot_product,
    get_dimension_values,
    get_mapbox_item_id,
    rm_special_characters,
)

import copy

#%%
# if __name__ == "__main__":
# hard-coded input params
GCS_PROJECT = "orelse" # Name of the google cloud project
BUCKET_NAME = "orelse-data-public" # Name of the bucket within the project
BUCKET_PROJ = "example" # Name of the project(folder) within the bucket
MAPBOX_PROJ = "henriqueguarneri" # Name of the mapbox username

# hard-coded input params at project level
coclico_data_dir = pathlib.Path(r"C:\Users\guarneri\local\postdoc\orelse\data")
dataset_dir = coclico_data_dir.joinpath("28_dis_2_1")
cred_dir = pathlib.Path(r"C:\Users\guarneri\local\postdoc\orelse\cloud_credentials")

DIR_NAME = "dis_2_1"

# TODO: safe cloud creds in password client
load_env_variables(env_var_keys=["MAPBOX_ACCESS_TOKEN"])
load_google_credentials(
    google_token_fp=cred_dir.joinpath("orelse-bdb839de9b42.json")
)

#%%

dir_to_google_cloud(
    dir_path= str(dataset_dir), 
    gcs_project= GCS_PROJECT,
    bucket_name= BUCKET_NAME,
    bucket_proj= BUCKET_PROJ,
    dir_name= DIR_NAME
    )

#%%
# cloud_optimized_geotiffs_to_mapbox
import subprocess

def cloud_optimized_geotiffs_to_mapbox(source_fpath: pathlib.Path, mapbox_url: str) -> None:
    if not source_fpath.exists():
        raise FileNotFoundError(f": {source_fpath} not found.")

    print(f"Writing to mapbox at {mapbox_url}")

    mapbox_cmd = r"mapbox --access-token {} upload {} {}".format(
        os.environ.get("MAPBOX_ACCESS_TOKEN", ""), mapbox_url, str(source_fpath)
    )
    # TODO: check if subprocess has to be run with check=True
    subprocess.run(mapbox_cmd, shell=True)

# %%
for file in filelist:
    # Open raster file
    
    # Project file (EPSG:23031)
    
    # Reproject file (EPSG:3857)
    