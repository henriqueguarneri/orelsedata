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

def get_geojson(ds, variable, dimension_combinations, stations_dim):
    # deep copy is required because pop will otherwise mutate global dimcombs dictionaries
    dimcombs = copy.deepcopy(dimension_combinations)

    da = ds[variable]
    #idxs = da[stations_dim].values.tolist()
    idxs = np.arange(ds.dims['Y'] * ds.dims['X'])

    # data with geometries can be read into geojson format directly, but features from data
    # with no geometry coordinates are constructed from lons/lats.
    if "geometry" in ds:
        features = [geojson.Feature(geometry=i.__geo_interface__) for i in ds.geometry.values]
    else:
        lons = da["Y"].values.tolist()
        lats = da["X"].values.tolist()
        geoms = [geojson.Point([lon, lat]) for lon, lat in zip(lons, lats)]
        features = [geojson.Feature(geometry=g) for g in geoms]

    # TODO: write into list comprehension
    for idx, feature in zip(idxs, features):
        feature["properties"]["locationId"] = idx

    # add variable values per mapbox layer to the geojson properties
    if dimcombs != []:
        for dimdict in dimcombs:
            da_ = (
                da.copy()
            )  # copy is required because each iteration da will be indexed
            mapbox_layer_id = get_mapbox_item_id(dimdict)

            # read dimensions that are coordinates from dataset because slice (sel) cannot be used
            # on non-index coordinates https://github.com/pydata/xarray/issues/2028
            for dim in list(dimdict.keys()):
                if dim not in da.dims:
                    dimkey = "n" + dim
                    if dimkey in da.dims:
                        da_ = da_.sel(
                            {
                                dimkey: list(da_[dimkey].values).index(
                                    list(ds[dim].values).index(dimdict.pop(dim))
                                )
                            }  # indexing goes well for strings and indices
                        )

            vals = da_.sel(dimdict).values.tolist()
            vals = [vals] if not isinstance(vals, list) else vals # Exception for single value
            for feature, value in zip(features, vals):
                feature["properties"][mapbox_layer_id] = value

    if dimcombs == []:  # add independent variable property for mapbox layer styling
        vals = da.values.tolist()
        for feature, value in zip(features, vals):
            feature["properties"][
                "value"
            ] = value  # TODO: might change 'value' into selected variable name later

    return geojson.FeatureCollection(features)

#%%
# if __name__ == "__main__":
# hard-coded input params
GCS_PROJECT = "orelse" # Name of the google cloud project
BUCKET_NAME = "orelse-data-public" # Name of the bucket within the project
BUCKET_PROJ = "example" # Name of the project(folder) within the bucket
MAPBOX_PROJ = "henriqueguarneri" # Name of the mapbox username

# hard-coded input params at project level
coclico_data_dir = pathlib.Path(r"C:\Users\guarneri\local\postdoc\orelse\data")
dataset_dir = coclico_data_dir.joinpath("26_dis_3_0")
cred_dir = pathlib.Path(r"C:\Users\guarneri\local\postdoc\orelse\cloud_credentials")

IN_FILENAME = "dis_3_0.zarr"  # original filename as on P drive
OUT_FILENAME = "dis_3_0.zarr"  # file name in the cloud and on MapBox

VARIABLES = ["KLASSE_DEF_SOARES"]  # what variable(s) do you want to show as marker color?
# dimensions to include, i.e. what are the dimensions that you want to use as to affect the marker color (never include stations). These will be the drop down menu's. Note down without n.. in front.
ADDITIONAL_DIMENSIONS = ['Z']  # False, None, or str; additional dims ""
# which dimensions to ignore (if n... in front of dim, it goes searching in additional_dimension for dim without n in front (ntime -> time). Except for nstations, just specify station in this case). This spans up the remainder of the dimension space.
DIMENSIONS_TO_IGNORE = ['X','Y']  # List of str; dims ignored by datacube
# use these to reduce dimension like {ensemble: "mean", "time": [1995, 2020, 2100]}, i.e. which of the dimensions do you want to use. Also specify the subsets (if there are a lot maybe make a selection). These will be the values in the drop down menu's. If only one (like mean), specify a value without a list to squeeze the dataset. Needs to span the entire dim space (except for (n)stations).
MAP_SELECTION_DIMS = {}

# TODO: safe cloud creds in password client
load_env_variables(env_var_keys=["MAPBOX_ACCESS_TOKEN"])
load_google_credentials(
    google_token_fp=cred_dir.joinpath("orelse-bdb839de9b42.json")
)

# TODO: come up with checks for data

# upload data to gcs from local drive
source_data_fp = dataset_dir.joinpath(IN_FILENAME)
#%%
# dataset_to_google_cloud(
#     ds=source_data_fp,
#     gcs_project=GCS_PROJECT,
#     bucket_name=BUCKET_NAME,
#     bucket_proj=BUCKET_PROJ,
#     zarr_filename=OUT_FILENAME,
# )
#%%
# read data from gcs
ds = dataset_from_google_cloud(
    bucket_name=BUCKET_NAME, bucket_proj=BUCKET_PROJ, zarr_filename=OUT_FILENAME
)
#%%
# # read data from local source
# fpath = pathlib.Path.home().joinpath(
#     "data", "tmp", "shoreline_change_projections.zarr"
# )
# ds = xr.open_zarr(fpath)

ds = zero_terminated_bytes_as_str(ds)

# remove characters that cause problems in the frontend.

ds = rm_special_characters(
    ds=ds, dimensions_to_check=ADDITIONAL_DIMENSIONS, characters=["%"]
)

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
for var in VARIABLES:
    collection = get_geojson(
        ds, variable=var, dimension_combinations=dimcombs, stations_dim="stations"
    )

    # save feature collection as geojson in tempdir and upload to cloud
    with dataset_dir.joinpath("platform") as outdir:
        # with tempfile.TemporaryDirectory() as outdir:

        # TODO: put this in a function because this is also used in generate_stac scripts?
        mapbox_url = get_mapbox_url(
            MAPBOX_PROJ, OUT_FILENAME, var, add_mapbox_protocol=False
        )

        fn = mapbox_url.split(".")[1]

        fp = pathlib.Path(outdir, fn).with_suffix(".geojson")

        # Create directory if necessary
        if not os.path.exists(os.path.dirname(str(fp))):
            os.mkdir(os.path.dirname(str(fp)))

        with open(fp, "w") as f:
            # load
            print(f"Writing data to {fp}")
            geojson.dump(collection, f)
        print("Done!")

        # Note, if mapbox cli raises an util collection error, this should be monkey
        # patched. Instructions are in documentation of the function.
        geojson_to_mapbox(source_fpath=fp, mapbox_url=mapbox_url)

# %%
