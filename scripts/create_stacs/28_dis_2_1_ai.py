#%%
import os
import pathlib
import sys
import json
from posixpath import join as urljoin

import pystac
from coclicodata.drive_config import p_drive
from coclicodata.etl.cloud_utils import dataset_from_google_cloud
from coclicodata.etl.extract import get_mapbox_url, zero_terminated_bytes_as_str
from pystac import Catalog, CatalogType, Collection, Summaries
from coclicodata.coclico_stac.io import CoCliCoStacIO
from coclicodata.coclico_stac.layouts import CoCliCoZarrLayout
from coclicodata.coclico_stac.templates import (
    extend_links,
    gen_default_collection_props,
    gen_default_item,
    gen_default_item_props,
    gen_default_summaries,
    gen_mapbox_asset,
    gen_zarr_asset,
    get_template_collection,
)
from coclicodata.coclico_stac.extension import CoclicoExtension
from coclicodata.coclico_stac.datacube import add_datacube
from coclicodata.coclico_stac.utils import (
    get_dimension_dot_product,
    get_dimension_values,
    get_mapbox_item_id,
    rm_special_characters,
)
from pathlib import Path
#%%
# if __name__ == "__main__":
# hard-coded input params at project level
BUCKET_NAME = "orelse-data-public" # Name of the bucket within the project
BUCKET_PROJ = "example" # Name of the project(folder) within the bucket
MAPBOX_PROJ = "henriqueguarneri" # Name of the mapbox username
STAC_DIR = "current"	# STAC directory
catalog_dir = Path(r"C:\Users\guarneri\OneDrive - Stichting Deltares\Documents\PostdocVault\Postdoc\ORELSE\Database\DIS2_1_Winbaarheidsgrids_COGs\Combi\catalog")

#%%
catalog = Catalog.from_file(os.path.join(pathlib.Path(__file__).parent.parent.parent, STAC_DIR, "catalog.json"))    
# %%
catalog_dis = Catalog.from_file(catalog_dir / "catalog.json")
#%%
import shutil
os.makedirs(os.path.join(pathlib.Path(__file__).parent.parent.parent, STAC_DIR, "dis2_1"),exist_ok=True)
shutil.copytree(catalog_dir , os.path.join(pathlib.Path(__file__).parent.parent.parent, STAC_DIR, "dis2_1" ),dirs_exist_ok=True)

catalog_dis21 = Catalog.from_file(os.path.join(pathlib.Path(__file__).parent.parent.parent, STAC_DIR, "dis2_1" ,"catalog.json"))
# %%
catalog_dis21.describe()
# %%
#Include assets path in google cloud
for item in catalog_dis21.get_all_items():
    for asset in item.assets.values():
        asset.href = urljoin("gs://", BUCKET_NAME, BUCKET_PROJ, asset.href)
        
        
#Create Mpabox assets
for item in catalog_dis21.get_all_items():
    for asset in item.assets.values():
        if asset.media_type == "image/tiff":
            asset = gen_mapbox_asset(asset, MAPBOX_PROJ)

#Push data to Mapbox
for item in catalog_dis21.get_all_items():
    for asset in item.assets.values():
        if asset.media_type == "image/tiff":
            dataset_to_mapbox(asset)

#Push the catalog and data to google cloud
#send data folder with COGs to google cloud, the whole folder because its already strucured correctly
dataset_to_google_cloud(catalog_dis21, BUCKET_NAME, BUCKET_PROJ, GC_DATA_DIR)
#send catalog to google cloud
catalog_to_google_cloud(catalog_dis21, BUCKET_NAME, BUCKET_PROJ, GC_CATALOG_DIR)
# %%


#