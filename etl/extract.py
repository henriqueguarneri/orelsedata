import pathlib
from importlib.resources import path
from multiprocessing.sharedctypes import Value

import geojson
import numpy as np
import rioxarray as rioxarray
import xarray as xr
from shapely import wkb
from shapely.geometry import mapping
from stac.utils import get_mapbox_item_id


def get_mapbox_url(mapbox_proj: str, filename: str, var: str) -> str:
    """Generate tileset name"""
    tilename = f"{pathlib.Path(filename).stem}_{var}"
    if len(tilename) > 32:
        raise ValueError("Mapbox tilenames cannot be longer than 32 characters.")
    return f"{mapbox_proj}.{tilename}"


def zero_terminated_bytes_as_str(ds: xr.Dataset) -> xr.Dataset:
    """Load zero-terminated bytes as strings

    To be CF compliant the coordinates of dtype string are stored as zero-terminated bytes. This
    function loads those bytes back to strings.

    Args:
        ds (xr.Dataset): Xarray dataset

    Returns:
        xr.Dataset: Xarray dataset
    """
    for coord in list(ds.coords):
        if np.issubdtype(ds[coord].dtype, np.dtype("S")):
            # TODO: type check to distinguish between strings and geometries in zero terminated bytes
            coord_dims = ds[coord].dims
            try:
                # strings are stored as zero-terminated bytes
                # ds.coords[coord] = ds.coords[coord].values.astype(str)

                ds = ds.assign_coords(
                    {coord: (coord_dims, ds[coord].values.astype(str))}
                )
            except:
                # geometries are stored as wkb
                ds = ds.assign_coords(
                    {coord: (coord_dims, list(map(wkb.loads, ds[coord].values)))}
                )
                # ds.coords[coord] = list(map(wkb.loads, ds.coords[coord].values))
    return ds


def clear_zarr_information(ds):
    """Zarr is inserting VLenUTF8 as a filter, but the loaded data array already has
    that as a filter so it's trying to double encode, see (https://github.com/pydata/xarray/issues/3476)
    """
    for v in list(ds.coords.keys()):
        if ds.coords[v].dtype == object:
            ds[v].encoding.clear()

            # updated because this also converts cftime objects; keep old solution as reference.
            # ds.coords[v] = ds.coords[v].astype("unicode")

    for v in list(ds.variables.keys()):
        if ds[v].dtype == object:
            ds[v].encoding.clear()

            # updated because this also converts cftime objects; keep old solution as reference.
            # ds[v] = ds[v].astype("unicode")

    return ds


def get_point_feature(idx, lon, lat):
    point = geojson.Point([lon, lat])
    point = geojson.Point([lon, lat])
    feature = geojson.Feature(geometry=point)
    feature["properties"]["locationId"] = idx
    return feature


def get_geojson(ds, variable, dimension_combinations, stations_dim):

    da = ds[variable]
    idxs = da[stations_dim].values.tolist()

    # data with geometries can be read into geojson format directoy, but features from data
    # wil no # geometry coordinates are constructed from lons/lats.
    if "geometry" in ds:
        features = [geojson.Feature(geometry=mapping(i)) for i in ds.geometry.values]
    else:
        lons = da["lon"].values.tolist()
        lats = da["lat"].values.tolist()
        features = [geojson.Point([lon, lat]) for lon, lat in zip(lons, lats)]

    # TODO: write into list comprehension
    for idx, feature in zip(idxs, features):
        feature["properties"]["locationId"] = idx

    # add variable values per mapbox layer to the geojson properties
    for dimdict in dimension_combinations:
        da_ = da.copy()  # copy is required because each iteration da will be indexed
        mapbox_layer_id = get_mapbox_item_id(dimdict)

        # read dimensions that are coordinates from dataset because slice (sel) cannot be used
        # on non-index coordinates https://github.com/pydata/xarray/issues/2028
        for dim in list(dimdict.keys()):
            if dim not in da.dims:
                dimkey = "n" + dim
                if dimkey in da.dims:
                    da_ = da_.sel({dimkey: dim == dimdict.pop(dim)})

        vals = da_.sel(dimdict).values.tolist()
        for feature, value in zip(features, vals):
            feature["properties"][mapbox_layer_id] = value

    return geojson.FeatureCollection(features)
