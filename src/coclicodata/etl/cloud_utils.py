import os
import pathlib
import subprocess
import tempfile
import warnings
from itertools import product
from posixpath import join as urljoin
from typing import Optional, Union

import gcsfs
import geojson
import xarray as xr
from dotenv import load_dotenv
from google.cloud import storage

from coclicodata.drive_config import p_drive, proj_dir
from coclicodata.etl.extract import clear_zarr_information


def _validate_fpath(*args: pathlib.Path) -> None:
    for fpath in args:
        if not isinstance(fpath, pathlib.Path):
            raise TypeError(
                f"Argument should be of type pathlib.Path, not {type(fpath)}."
            )

        if not fpath.exists():
            raise FileNotFoundError(f"{fpath} does not exist.")


def dataset_to_google_cloud(ds, gcs_project, bucket_name, bucket_proj, zarr_filename):
    """Upload zarr store to Google Cloud Services

    # TODO: fails when uploading to store that already exists, UPDATE; not for Windows OS

    """

    if isinstance(ds, pathlib.Path):
        _validate_fpath(ds.parent, ds)

        #  TODO: append to cloud zarr store from Dask chunks in parallel
        ds = xr.open_zarr(ds)

        # zarr tries to double encode some information
        ds = clear_zarr_information(ds)

    # file system interface for google cloud storage
    fs = gcsfs.GCSFileSystem(
        gcs_project, token=os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    )

    target_path = urljoin(bucket_name, bucket_proj, zarr_filename)

    gcsmap = gcsfs.mapping.GCSMap(target_path, gcs=fs)

    print(f"Writing to zarr store at {target_path}...")
    try:
        ds.to_zarr(store=gcsmap, mode="w")
        print("Done!")
    except OSError as e:
        print(f"Failed uploading: \n {e}")


def dataset_from_google_cloud(bucket_name, bucket_proj, zarr_filename):
    uri = urljoin("gs://" + bucket_name, bucket_proj, zarr_filename)
    return xr.open_zarr(uri)


def dir_to_google_cloud(
    dir_path: str, gcs_project: str, bucket_name: str, bucket_proj: str, dir_name: str
) -> None:
    """Upload directory to Google Cloud Services

    # TODO: fails when uploading to store that already exists or;
    # TODO: creates a subfolder in the desired folder when a store already exists, fix this..

    """

    # file system interface for google cloud storage
    fs = gcsfs.GCSFileSystem(
        gcs_project, token=os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    )

    target_path = urljoin(bucket_name, bucket_proj, dir_name)

    # saved directory to google cloud
    print(f"Writing to directory at {target_path}...")
    try:
        fs.put(dir_path, target_path, recursive=True)
        print("Done!")
    except OSError as e:
        print(f"Failed uploading: \n {e}")


def geojson_to_mapbox(source_fpath: pathlib.Path, mapbox_url: str) -> None:
    """Upload GeoJSON to Mapbox by CLI.

    Mapbox Python SDK recommends to use Mapbox Python CLI, but CLI seems outdated.

    # TODO: make PR at mapbox gh?
    Installing Mapbox CLI in Python 3.10 raises 'Cannot import mapping from collection.'
    This can be resolved by patching the following in the Mapbox package:

    from collections import Mapping

    to

    from collections.abc import Mapping

    Note 18/8/2022: monkey patch is still required for mapboxcli package. This package
    is also not available on conda-forge and was installed with pip.

    """

    if not source_fpath.exists():
        raise FileNotFoundError(f": {source_fpath} not found.")

    print(f"Writing to mapbox at {mapbox_url}")

    mapbox_cmd = r"mapbox --access-token {} upload {} {}".format(
        os.environ.get("MAPBOX_ACCESS_TOKEN", ""), mapbox_url, str(source_fpath)
    )
    # TODO: check if subprocess has to be run with check=True
    subprocess.run(mapbox_cmd, shell=True)


class CredentialLeakageWarning(Warning):
    pass


def load_env_variables(env_var_keys: list = list()) -> None:
    warnings.warn(
        (
            "This function will be deprecated in the future, please the pacakge"
            " python_dotenv to load environment variables."
        ),
        FutureWarning,
    )
    env_fpath = proj_dir.joinpath(".env")
    if not env_fpath.exists():
        raise FileNotFoundError(
            "Processing data requires access keys for cloud services, which should be"
            f" stored as environment variables in .../{proj_dir.joinpath('.env')}"
        )

    load_dotenv(env_fpath)
    for env_var in env_var_keys:
        if not env_var in os.environ:
            raise KeyError(f"{env_var} not in environmental variables.")
    print("Environmental variables loaded.")


def load_google_credentials(google_token_fp: Union[pathlib.Path, None] = None) -> None:
    warnings.warn(
        (
            "This function will be deprecated in the future, please use environment"
            " variables instead. When Google cloud is installed on your computer"
            " credentials can set using 'GOOGLE_DEFAULT' in the storage_kwargs argument"
        ),
        FutureWarning,
    )

    if google_token_fp:
        #  TODO: Manage keys at user level, not with shared drive. The code block below
        # should tested by Windows users to see if gcloud credentials behave similar on
        # that os. If so, the code block below can be used instead.
        warnings.warn(
            "Keys loaded from shared network drive.", CredentialLeakageWarning
        )
        if not google_token_fp.exists():
            if not p_drive.exists():
                raise FileNotFoundError(
                    "Deltares drive not found, mount drive to access Google keys."
                )
            raise FileNotFoundError("Credential file does not exist.")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(google_token_fp)
        print("Google Application Credentials load into environment.")

    else:
        #  TODO: Migrate to token=None in gcsfs when bug that no credentials can be found is fixed.
        gmail_pattern = "*@gmail.com"
        p = pathlib.Path.home().joinpath(
            ".config", "gcloud", "legacy_credentials", gmail_pattern, "adc.json"
        )
        p = list(p.parent.parent.expanduser().glob(p.parent.name))[0].joinpath(p.name)
        if not p.exists():
            raise FileNotFoundError("Google credentials not found.")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p)
        print("Google Application Credentials load into environment.")


if __name__ == "__main__":
    # hard-coded input params
    DATASET_FILENAME = "CoastAlRisk_Europe_EESSL.zarr"
    GCS_PROJECT = "DGDS - I1000482-002"
    BUCKET_NAME = "dgds-data-public"
    BUCKET_PROJ = "coclico"

    # semi hard-coded variables including both local and remote drives
    coclico_data_dir = pathlib.Path(p_drive, "11205479-coclico", "data")
    network_dir = coclico_data_dir.joinpath("06_adaptation_jrc")
    local_dir = pathlib.Path.home().joinpath("ddata", "temp")

    # TODO: safe cloud creds in password client
    load_env_variables(env_var_keys=["MAPBOX_ACCESS_TOKEN"])
    load_google_credentials(
        google_token_fp=coclico_data_dir.joinpath("google_credentials.json")
    )

    # # commented code is here to provide an example of how this file can be used as a script to
    # # interact with cloud services.

    # # upload data to cloud from local drive
    # source_data_fp = local_dir.joinpath(DATASET_FILENAME)

    # dataset_to_google_cloud(
    #     ds=source_data_fp,
    #     gcs_project=GCS_PROJECT,
    #     bucket_name=BUCKET_NAME,
    #     bucket_proj=BUCKET_PROJ,
    #     zarr_filename=DATASET_FILENAME,
    # )

    # # read data from cloud
    # ds = dataset_from_google_cloud(
    #     bucket_name=BUCKET_NAME, bucket_proj=BUCKET_PROJ, zarr_filename=DATASET_FILENAME
    # )
    # )
