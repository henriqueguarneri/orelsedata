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
# %%

import rioxarray
import rasterio
from rasterio.enums import Resampling
from rasterio.shutil import copy

def convert_asc_to_cog_(input_asc_path, output_cog_path, overview_resampling = Resampling.average, target_crs="EPSG:23030"):
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
            dst.build_overviews([2, 4, 8, 16, 32, 64], overview_resampling)
            dst.update_tags(ns='rio_overview', resampling=overview_resampling.name)
    # Copy to a new file with COG metadata and structure
    copy(root_data / output_cog_path, root_data / output_cog_path.with_name(output_cog_path.stem + "_cog.tiff"), copy_src_overviews=True, **profile)

    print(f"COw created at: {output_cog_path}")
    
#%%
root_data = Path(r"P:\11209126-orelse\4_Database\bathymetry\WUR")
input_path = "NCP_Grid5m_WUR-ETRS89-UTMzone31N-LAT.txt"
output_path = "NCP_Grid5m_WUR-ETRS89-UTMzone31N-LAT.tif"
#%%
                 
rds = rioxarray.open_rasterio(root_data / input_path)
# %%

import pandas as pd
import rioxarray
import matplotlib.pyplot as plt

# Step 1: Load the CSV data
data = pd.read_csv(root_data/input_path, header=None, names=["x", "y", "value"], delim_whitespace=True)

#%%
# Step 2: Pivot the DataFrame to create a 2D grid of values
grid = data.pivot_table(index="y", columns="x", values="value", dropna=False)

# Step 3: Convert the pivoted DataFrame to an xarray DataArray
raster = grid.to_xarray()

# Step 4: Assign CRS (Coordinate Reference System) to the DataArray
# Replace 'EPSG:4326' with the appropriate CRS for your data
raster.rio.write_crs("EPSG:4326", inplace=True)

# Step 5: Export the DataArray to a GeoTIFF file
raster.rio.to_raster("output.tif")

# Optional: Plotting the data
plt.imshow(raster)
plt.colorbar()
plt.show()
