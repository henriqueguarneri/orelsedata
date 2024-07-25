#%%
import datetime
import itertools
import math
import operator
import os
import pathlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import dask
import fsspec
import numpy as np
import pandas as pd
import pooch
import pystac
import rasterio
import rioxarray
import shapely
import xarray as xr
from azure.storage.blob import BlobServiceClient
from dask.distributed import Client
from dotenv import load_dotenv
from gcsfs import GCSFileSystem
from pystac import StacIO
from pystac.extensions import eo, raster
from pystac.layout import BestPracticesLayoutStrategy
from pystac.utils import JoinType, join_path_or_url, safe_urlparse
from stactools.core.utils import antimeridian
from tqdm import tqdm
import json
#%%
# uncomment these lines if you do not have coclicodata in development mode installed
# dev_dir = pathlib.Path.home() / "dev"  # set the path to the location where you would like to clone the package
# dev_dir.mkdir(parents=True, exist_ok=True)

# # Clone the repository
# os.system(f"git clone https://github.com/openearth/coclicodata.git {dev_dir / 'coclicodata'}")

# # Install the package in development mode
# os.system(f"pip install -e {dev_dir / 'coclicodata'}")

from coclicodata.coclico_stac.extension import CoclicoExtension
from coclicodata.coclico_stac.io import CoCliCoStacIO
from coclicodata.coclico_stac.layouts import CoCliCoCOGLayout
from coclicodata.drive_config import p_drive

#%%

import sys
sys.path.append(r"C:\Users\guarneri\OneDrive - Stichting Deltares\Documents\PostdocVault\Postdoc\ORELSE\Database\DIS2_1_Winbaarheidsgrids_COGs\Combi\catalog")
import content

path_ = Path(r"C:\Users\guarneri\OneDrive - Stichting Deltares\Documents\PostdocVault\Postdoc\ORELSE\Database\DIS2_1_Winbaarheidsgrids_COGs\Combi")
data_path = pathlib.Path(r"C:\Users\guarneri\OneDrive - Stichting Deltares\Documents\PostdocVault\Postdoc\ORELSE\Database\DIS 2.1 - Winbaarheidsgrids")

with open(path_ / 'directory_tree.json', 'r') as file:
    directory_tree = json.load(file)
global root_data
root_data = Path(r'C:\Users\guarneri\OneDrive - Stichting Deltares\Documents\PostdocVault\Postdoc\ORELSE\Database\DIS2_1_Winbaarheidsgrids_COGs\Combi')

OUT_DIR = pathlib.Path.home() / "data" / "tmp" / "28_dis_2_1"
STAC_DIR = pathlib.Path.cwd().parent / "current"

if not OUT_DIR.exists():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

HREF_PREFIX = OUT_DIR  # test the workflow by writing to a local filesystem

def create_item(block, item_id, antimeridian_strategy=antimeridian.Strategy.SPLIT):
    """Create COG item from xarray DataArray block.

    Args:
        block (xarray.core.dataarray.DataArrayrio.xarray): _description_\
        item_id (str): item id
        antimeridian_strategy (<enum 'Strategy'>, optional): _description_. Defaults to antimeridian.Strategy.SPLIT.

    Returns:
        pystac.item.Item: STAC item
    """
    dst_crs = rasterio.crs.CRS.from_epsg(4326)

    # when the data spans a range, it's common practice to use the middle time as the datetime provided
    # in the STAC item. So then you have to infer the start_datetime, end_datetime and get the middle
    # from those.
    # start_datetime, end_datetime = ...
    # middle_datetime = start_datetime + (end_datetime - start_datetime) / 2

    # the bbox of the STAC item is provided in 4326
    #bbox = rasterio.warp.transform_bounds(block.rio.crs, dst_crs, *block.rio.bounds())
    bbox = rasterio.warp.transform_bounds("EPSG:23031", "EPSG:4326", *block.rio.bounds())
    geometry = shapely.geometry.mapping(shapely.make_valid(shapely.geometry.box(*bbox)))
    bbox = shapely.make_valid(shapely.box(*bbox)).bounds

    item = pystac.Item(
        id=item_id,
        geometry=geometry,
        bbox=bbox,
        datetime=datetime.datetime.now(),
        properties={},
    )

    # useful for global datasets that cross the antimerdian E-W line
    antimeridian.fix_item(item, antimeridian_strategy)

    # use this when the data spans a certain time range
    # item.common_metadata.start_datetime = start_datetime
    # item.common_metadata.end_datetime = end_datetime

    item.common_metadata.created = datetime.datetime.utcnow()

    ext = pystac.extensions.projection.ProjectionExtension.ext(
        item, add_if_missing=True
    )
    ext.bbox = block.rio.bounds()  # these are provided in the crs of the data
    ext.shape = tuple(v for k, v in block.sizes.items() if k in ["y", "x"])
    ext.epsg = "EPSG:23031" #block.rio.crs.to_epsg()
    ext.geometry = shapely.geometry.mapping(shapely.geometry.box(*ext.bbox))
    ext.transform = list(block.rio.transform())[:6]
    ext.add_to(item)

    # add CoCliCo frontend properties to visualize it in the web portal
    # TODO: This is just example. We first need to decide which properties frontend needs for COG visualization
    coclico_ext = CoclicoExtension.ext(item, add_if_missing=True)
    coclico_ext.item_key = item_id
    coclico_ext.add_to(item)

    # add more functions to describe the data at item level, for example the frontend properties to visualize it
    ...

    return item
#%%
# Assuming this is a function you have that converts ASC files to COG
def convert_asc_to_cog(asc_file_path, asd_coordsys, cog_file_path=None):
    """Convert ASC file to COG file.
    Args:
        asc_file_path (Path object): Path to the ASC file.	
        asd_coordsys (str): Coordinate system of the ASC file.
    Returns:
        str: Path to the COG file.
    """
    if cog_file_path is None:
        asc_file_path = Path(asc_file_path) # Ensure it's a Path object
        cog_file_path = asc_file_path.with_suffix('.tif')# Placeholder conversion logic
    else:
        cog_file_path = cog_file_path # Ensure it's a Path object
    
    if not os.path.exists(cog_file_path):
        pass
    else:
              
        if os.path.exists(root_data / cog_file_path):
            print(f"File {cog_file_path} already exists in the directory it will be overwritten")
        # Placeholder conversion logic
        asc_file = rioxarray.open_rasterio(root_data / Path(asc_file_path))
        #define the coordinate reference system
        asc_file.rio.set_crs(asd_coordsys)
        asc_file.squeeze().rio.to_raster(root_data / cog_file_path, driver="COG")
        # Log the successful conversion based on cog_file existence cathing the exception     
        #Get cog_file_path timestamp after conversion
        if os.path.exists(root_data / cog_file_path):
            print(f"Converted {asc_file_path} to {cog_file_path}")

    return cog_file_path

import rioxarray
import rasterio
from rasterio.enums import Resampling
from rasterio.shutil import copy

def convert_asc_to_cog_(input_asc_path, output_cog_path, overview_resampling = Resampling.average, target_crs="EPSG:23031"):
    # Read the data
    input_asc_path = Path(input_asc_path)
    output_cog_path = Path(output_cog_path)
    
    rds = rioxarray.open_rasterio(root_data / input_asc_path)
    
    # Check if CRS is set, if not, set it
    if rds.rio.crs is None:
        rds.rio.write_crs(target_crs, inplace=True)
    else:
        print(f"Original CRS: {rds.rio.crs}")
        
    # You can perform operations on rds here if necessary (e.g., clipping, rescaling)

    # Define the output profile for the COG
    with rasterio.open(root_data / input_asc_path) as src:
        profile = src.profile

    # Updating the profile to output a COG
    profile.update(driver='GTiff',
                   dtype='float32',  # Ensure dtype matches your data's needs
                   compress='lzw',   # Compression
                   tiled=True,       # Necessary for COG
                   blockxsize=256,   # Tile size, adjust as needed
                   blockysize=256,   # Tile size, adjust as needed
                   num_threads='all_cpus',
                   predictor=2,
                   bigtiff='if_safer',
                   crs=rds.rio.crs.to_wkt()
    )        

    # Writing to COG
    with rasterio.open(root_data / output_cog_path, 'w', **profile) as dst:
        for i in range(1, rds.shape[0] + 1):  # Loop over each band
            # Read data for the current band
            band_data = rds.sel(band=i).values

            # Write the band data
            dst.write(band_data, i)
            # Implement overviews (pyramids)
            dst.build_overviews([2, 4, 8, 16, 32], overview_resampling)
            dst.update_tags(ns='rio_overview', resampling=overview_resampling.name)
    # Copy to a new file with COG metadata and structure
    copy(root_data / output_cog_path, root_data / output_cog_path.with_name(output_cog_path.stem + "_cog.tiff"), copy_src_overviews=True, **profile)

    print(f"COG created at: {output_cog_path}")



def create_item_(file_info):
    from urllib.parse import urljoin
    # Assuming conversion to COG has already been done, and `cog_path` is available
    cog_path = file_info['path'].replace('.asc', '.tif')
    item_id = file_info['name'].replace('.asc', '')
    item = create_item(rioxarray.open_rasterio(path_ / cog_path),item_id)
    # item = pystac.Item(id=item_id,
    #                    geometry=None,  # Placeholder, replace with actual geometry
    #                    bbox=None,  # Placeholder, replace with actual bbox
    #                    datetime=datetime.datetime.now(),  # Placeholder, set a datetime if available
    #                    properties={})
    item.add_asset("data", pystac.Asset(href = urljoin(HREF_PREFIX,  cog_path), media_type=pystac.MediaType.COG))
    return item

def create_collection_from_directory(directory, parent_catalog):
    collection_id = directory['name']

    providers = [
        pystac.Provider(
            name="Deltares",
            roles=[
                pystac.provider.ProviderRole.PROCESSOR,
            ],
            url="https://www.deltares.nl/expertise/projecten/zand-uit-de-noordzee/",
        ),
        pystac.Provider(
            "TNO",
            roles=[
                pystac.provider.ProviderRole.PRODUCER,
                pystac.provider.ProviderRole.HOST,
            ],
            url="https://www.dinoloket.nl/dis",
        ),
        pystac.Provider(
            "Rijkswaterstaat",
            roles=[
                pystac.provider.ProviderRole.PRODUCER,
            ],
            url="https://maps.rijkswaterstaat.nl/gwproj55/index.html?viewer=ZD_Zandwinstrategie.Webviewer",
        )
    ]

    keywords = content.root_catalog['keywords']
    
    collection = pystac.Collection(
        id=collection_id,
        description=f"Collection for {collection_id}",
        keywords=keywords,
        providers=providers,
        extent=[None,None],
    )
    
    for file_info in directory.get('files', []):
        if file_info['name'].endswith('.asc'):
            #cog_path = convert_asc_to_cog(file_info['path'],"EPSG:23031")  # Convert ASC to COG
            convert_asc_to_cog_(file_info['path'], file_info['path'].replace('.asc', '.tif'))
            item = create_item_(file_info)
            collection.add_item(item)
 
    #collection.extent=item.extent
    spatial_extent = pystac.SpatialExtent(bboxes=[item.bbox])
    
    # Define the temporal extent of the collection
    from datetime import timezone, datetime
    
    start_date = datetime(2000, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2020, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    temporal_extent = pystac.TemporalExtent(intervals=[[start_date, end_date]])
    
    # Combine into an Extent object
    extent = pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)
    collection.extent = extent
    
    parent_catalog.add_child(collection)

def process_directory(directory, parent_catalog):
    if 'directories' in directory:
        for subdir in directory['directories']:
            if subdir.get('files'):
                create_collection_from_directory(subdir, parent_catalog)
            else:
                # Create a new catalog for this directory and process its subdirectories
                new_catalog = pystac.Catalog(
                    id=content.dict_names[subdir['name']]['id'],
                    description= content.dict_names[subdir['name']]['description']
                    )
                parent_catalog.add_child(new_catalog)
                process_directory(subdir, new_catalog)

#%%
# if __name__ == "__main__":

# uncomment line below if you want to write to the cloud
HREF_PREFIX = "https://storage.googleapis.com/dgds-data-public/coclico/coastal_mask"
HREF_PREFIX = "https://storage.googleapis.com/orelse-data-public/example/dis_2_1/dis_2_1/"
HREF_DIS_2_1 = "https://open.rijkswaterstaat.nl/publish/pages/143965/update_delfstoffen_informatie_systeem_dis_2-1.pdf"

# Initialize the root catalog
root_catalog = pystac.Catalog(
    id=content.dict_names['root']['id'],
    description=content.dict_names['root']['description'],
    title=content.dict_names['root']['title'],
    stac_extensions=['collection'],
    catalog_type=pystac.CatalogType.SELF_CONTAINED,
    )

# Process the directory structure to create the catalog hierarchy
process_directory(directory_tree, root_catalog)

# The root_catalog now contains the entire STAC catalog structure as described

# %%
# Define the path where you want to save the STAC catalog
save_directory = STAC_DIR / "dis2_1" / "catalog"
save_directory.mkdir(exist_ok=True)

# Normalize and save the catalog
# This operation will save the root catalog and all nested catalogs, collections, and items
# to the specified directory.
root_catalog.normalize_and_save(save_directory.__str__(), catalog_type=pystac.CatalogType.SELF_CONTAINED)

# %%

from pystac import Catalog, CatalogType, Collection, Summaries

catalog = Catalog.from_file(os.path.join(pathlib.Path(__file__).parent.parent.parent, STAC_DIR, "catalog.json"))


catalog.remove_child("dis_2_1_winbaarheidgrids")

catalog.add_child(root_catalog)
COLLECTION_ID = content.dict_names['root']['id']
from coclicodata.coclico_stac.layouts import CoCliCoCOGLayout
layout = CoCliCoCOGLayout()

root_catalog.normalize_hrefs(
    os.path.join(pathlib.Path(__file__).parent.parent.parent, STAC_DIR, COLLECTION_ID), strategy=layout
)


catalog.description = """Faced with rising sea levels and looming sand scarcity, our interdisciplinary project OR ELSE is committed to sustainable sand extraction. During this 5-year research programme on the North Sea floor, we explore the impact on the ecosystem while protecting our coast. Find out on our site how we are working towards a resilient future.

This STAC provides the structured data input and output of the ORELSE project. STATUS: Under development. """

catalog.save(
    catalog_type=CatalogType.SELF_CONTAINED,
    dest_href=os.path.join(pathlib.Path(__file__).parent.parent.parent, STAC_DIR),
    stac_io=CoCliCoStacIO(),
)
# %%
# %%
catalog.describe()
# %%
