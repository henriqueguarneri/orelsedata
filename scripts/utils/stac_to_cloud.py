#%%
import pathlib
import sys
import pystac
import pystac_client
import os

# make modules importable when running this file as script
sys.path.append(str(pathlib.Path(__file__).parent.parent))

from coclicodata.etl.cloud_utils import dir_to_google_cloud, load_google_credentials
from coclicodata.drive_config import p_drive
#%%
if __name__ == "__main__":
    # hard-coded input params
    GCS_PROJECT = "orelse" # Name of the google cloud project
    BUCKET_NAME = "orelse-data-public" # Name of the bucket within the project
    BUCKET_PROJ = "example" # Name of the project(folder) within the bucket
    STAC_NAME = "orelse-stac" # Name of the folder that will be created in the bucket
    IN_DIRNAME = "current" # Name of the folder that will be uploaded to the bucket

    # hard-coded input params at project level
    coclico_data_dir = pathlib.Path(r"C:\Users\guarneri\local\postdoc\orelse\data")
    cred_dir = pathlib.Path(r"C:\Users\guarneri\local\postdoc\orelse\cloud_credentials")

    # upload dir to gcs from local drive
    source_dir_fp = str(
        pathlib.Path(__file__).parent.parent.parent.joinpath(IN_DIRNAME)
    )

    # load google credentials
    load_google_credentials(
        google_token_fp=cred_dir.joinpath("orelse-bdb839de9b42.json")
    )

    # validate STAC catalog and upload to cloud
    catalog = pystac_client.Client.open(
        os.path.join(source_dir_fp, "catalog.json")  # local cloned STAC
    )

    # TODO: fix STAC validation to work properly with pystac >1.8
    # if catalog.validate_all() == None:  # no valid STAC
    #     print(
    #         "STAC is not valid and hence not uploaded to cloud, please adjust"
    #         " accordingly"
    #     )
    # else:
    dir_to_google_cloud(
        dir_path=source_dir_fp,
        gcs_project=GCS_PROJECT,
        bucket_name=BUCKET_NAME,
        bucket_proj=BUCKET_PROJ,
        dir_name=STAC_NAME,
    )
# %%
