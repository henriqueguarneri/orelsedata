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
#%%
#if __name__ == "__main__":
# hard-coded input params at project level
BUCKET_NAME = "orelse-data-public" # Name of the bucket within the project
BUCKET_PROJ = "example" # Name of the project(folder) within the bucket
MAPBOX_PROJ = "henriqueguarneri" # Name of the mapbox username

STAC_DIR = "current"
TEMPLATE_COLLECTION = "template"  # stac template for dataset collection
COLLECTION_ID = "dis3_2"  # name of stac collection
COLLECTION_TITLE = "DIS 3.0 alternative"  # title of stac collection
DATASET_DESCRIPTION = """The Delfstoffen Informatie Systeem (DIS), developed by Deltares and TNO for Rijkswaterstaat, provides insights into the distribution of sand, peat, clay, and silt in the subsurface, as well as the granularity of sand across the North Sea and coastal areas. It's a decision-support tool for sustainable development in these regions. DIS 3.0 is the latest version, enhancing geological complexity understanding, with Zeeland's model delivered in 2020 and plans for further regional models. Usage is subject to terms ensuring responsible application and acknowledging potential limitations at smaller scales. For more detailed information, visit DINOloket.nl."""
#%%
# hard-coded input params which differ per dataset

DATASET_FILENAME = "dis_3_0.zarr"
VARIABLES = ["KLASSE_DEF_SOARES"]  # xarray variables in dataset
X_DIMENSION = "X"  # False, None or str; spatial lon dim used by datacube
Y_DIMENSION = "Y"  # False, None or str; spatial lat dim ""
Z_DIMENSION = "Z"  # False, None or str; spatial lat dim ""
TEMPORAL_DIMENSION = False # False, None or str; temporal ""
ADDITIONAL_DIMENSIONS = ['Z']  # False, None, or str; additional dims ""
DIMENSIONS_TO_IGNORE = ['X','Y']  # List of str; dims ignored by datacube
MAP_SELECTION_DIMS = {}
STATIONS = {}
TYPE = "circle"
ON_CLICK = {}
#%%
# # these are added at collection level
# UNITS = "adimensional"
# PLOT_SERIES = "scenario"
# PLOT_X_AXIS = "time"
# PLOT_TYPE = "area"
# MIN = 0
# MAX = 3
# LINEAR_GRADIENT = [
#     {"color": "hsl(0,90%,80%)", "offset": "0.000%", "opacity": 100},
#     {"color": "hsla(55,88%,53%,0.5)", "offset": "50.000%", "opacity": 100},
#     {"color": "hsl(110,90%,70%)", "offset": "100.000%", "opacity": 100},
# ]

# functions to generate properties that vary per dataset but cannot be hard-corded because
# they also require input arguments
def get_paint_props(item_key: str):
    return {
        "circle-color": [
            "interpolate",
            ["linear"],
            ["get", item_key],
            0,
            "hsl(110,90%,80%)",
            1.5,
            "hsla(55, 88%, 53%, 0.5)",
            3.0,
            "hsl(0, 90%, 70%)",
        ],
        "circle-radius": [
            "interpolate",
            ["linear"],
            ["zoom"],
            0,
            0.5,
            1,
            1,
            5,
            5,
        ],
    }

# semi hard-coded input params
gcs_zarr_store = os.path.join("gcs://", BUCKET_NAME, BUCKET_PROJ, DATASET_FILENAME)
gcs_api_zarr_store = os.path.join(
    "https://storage.googleapis.com", BUCKET_NAME, BUCKET_PROJ, DATASET_FILENAME
)

# # read data from gcs zarr store
# ds = dataset_from_google_cloud(
#     bucket_name=BUCKET_NAME, bucket_proj=BUCKET_PROJ, zarr_filename=DATASET_FILENAME
# )
#%%
import xarray as xr

fpath = pathlib.Path.home().joinpath("data", "tmp", DATASET_FILENAME)
fpath = r"c:\Users\guarneri\local\postdoc\orelse\data\26_dis_3_0\dis_3_0.zarr"
ds = xr.open_zarr(fpath)
# subsampling_step = 20  # This means every 20th element, effectively "every other 10th" considering 0-based indexing
# ds_subsampled = ds.isel(X=slice(None, None, subsampling_step), 
#                         Y=slice(None, None, subsampling_step), 
#                         Z=slice(None, None, subsampling_step))
# ds = ds_subsampled
#%%
# cast zero terminated bytes to str because json library cannot write handle bytes
ds = zero_terminated_bytes_as_str(ds)

title = ds.attrs.get("title", COLLECTION_ID)

# load coclico data catalog
catalog = Catalog.from_file(os.path.join(pathlib.Path(__file__).parent.parent.parent, STAC_DIR, "catalog.json"))

template_fp = os.path.join(
    pathlib.Path(__file__).parent.parent.parent, STAC_DIR, TEMPLATE_COLLECTION, "collection.json"
)
#%%
# generate collection for dataset
collection = get_template_collection(
    template_fp=template_fp,
    collection_id=COLLECTION_ID,
    title=COLLECTION_TITLE,
    description=DATASET_DESCRIPTION,
    keywords=[],
)
#%%
# add datacube dimensions derived from xarray dataset to dataset stac_obj
collection = add_datacube(
    stac_obj=collection,
    ds=ds,
    x_dimension=X_DIMENSION,
    y_dimension=Y_DIMENSION,
    temporal_dimension=False,
    additional_dimensions=ADDITIONAL_DIMENSIONS,
)
#temporal_dimension=Z_DIMENSION,
#%%
# This dataset has quite some dimensions, so if we would parse all information the end-user
# would be overwhelmed by all options. So for the stac items that we generate for the frontend
# visualizations a subset of the data is selected. Of course, this operation is dataset specific.
for k, v in MAP_SELECTION_DIMS.items():
    if k in ds.dims and ds.coords:
        ds = ds.sel({k: v})
    else:
        try:
            # assume that coordinates with strings always have same dim name but with n
            ds = ds.sel({"n" + k: k == v})
        except:
            raise ValueError(f"Cannot find {k}")

dimvals = get_dimension_values(ds, dimensions_to_ignore=DIMENSIONS_TO_IGNORE)
dimcombs = get_dimension_dot_product(dimvals)
#%%
# TODO: check what can be customized in the layout
layout = CoCliCoZarrLayout()

# create stac collection per variable and add to dataset collection
for var in VARIABLES:
    print(var)
    # add zarr store as asset to stac_obj
    collection.add_asset("data", gen_zarr_asset(title, gcs_api_zarr_store))

    # stac items are generated per AdditionalDimension (non spatial)
    for dimcomb in dimcombs:
        print(dimcomb)
        mapbox_url = get_mapbox_url(MAPBOX_PROJ, DATASET_FILENAME, var)

        # generate stac item key and add link to asset to the stac item
        item_id = get_mapbox_item_id(dimcomb)
        feature = gen_default_item(f"{var}-mapbox-{item_id}")
        feature.add_asset("mapbox", gen_mapbox_asset(mapbox_url))

        # This calls ItemCoclicoExtension and links CoclicoExtension to the stac item
        coclico_ext = CoclicoExtension.ext(feature, add_if_missing=True)

        coclico_ext.item_key = item_id
        coclico_ext.paint = get_paint_props(item_id)
        coclico_ext.type_ = TYPE
        coclico_ext.stations = STATIONS
        coclico_ext.on_click = ON_CLICK

        # some datasets are reduced for frontend along certain dimension. Add that
        # dimension to the properties
        for k, v in MAP_SELECTION_DIMS.items():
            if k not in dimcomb:
                feature.properties[k] = v

        # TODO: include this in our datacube?
        # add dimension key-value pairs to stac item properties dict
        for k, v in dimcomb.items():
            feature.properties[k] = v

        # add stac item to collection
        collection.add_item(feature, strategy=layout)
#%%
# if no variables present we still need to add zarr reference at collection level
if not VARIABLES:
    collection.add_asset("data", gen_zarr_asset(title, gcs_api_zarr_store))

# TODO: use gen_default_summaries() from blueprint.py after making it frontend compliant.
collection.summaries = Summaries({})
# TODO: check if maxcount is required (inpsired on xstac library)
# stac_obj.summaries.maxcount = 50
for k, v in dimvals.items():
    collection.summaries.add(k, v)

# this calls CollectionCoclicoExtension since stac_obj==pystac.Collection
coclico_ext = CoclicoExtension.ext(collection, add_if_missing=True)
#%%
# Add frontend properties defined above to collection extension properties. The
# properties attribute of this extension is linked to the extra_fields attribute of
# the stac collection.
# coclico_ext.units = UNITS
# coclico_ext.plot_series = PLOT_SERIES
# coclico_ext.plot_x_axis = PLOT_X_AXIS
# coclico_ext.plot_type = PLOT_TYPE
# coclico_ext.min_ = MIN
# coclico_ext.max_ = MAX
# coclico_ext.linear_gradient = LINEAR_GRADIENT

# set extra link properties
extend_links(collection, dimvals.keys())

# add reduced dimensions as links as well
extend_links(
    collection,
    {k: v for k, v in MAP_SELECTION_DIMS.items() if k not in dimvals.keys()}.keys(),
)
#%%
# Add thumbnail
# collection.add_asset(
#     "thumbnail",
#     pystac.Asset(
#         "https://storage.googleapis.com/dgds-data-public/coclico/assets/thumbnails/" + COLLECTION_ID + ".png",  # noqa: E501
#         title="Thumbnail",
#         media_type=pystac.MediaType.PNG,
#     ),
# )

catalog.add_child(collection)

collection.normalize_hrefs(
    os.path.join(pathlib.Path(__file__).parent.parent.parent, STAC_DIR, COLLECTION_ID), strategy=layout
)

catalog.save(
    catalog_type=CatalogType.SELF_CONTAINED,
    dest_href=os.path.join(pathlib.Path(__file__).parent.parent.parent, STAC_DIR),
    stac_io=CoCliCoStacIO(),
)

# %%
