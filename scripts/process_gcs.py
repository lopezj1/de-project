# %%
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile, is_zipfile
import pandas as pd
from prefect import flow, task
from google.cloud import storage
import os

# %%
@task()
def read_gcs_bucket(bucket) -> list:
    """Get list of zip folders in GCS bucket"""
    lsblob = list(bucket.list_blobs(prefix="zip"))  # get list of blobs in zip folder of bucket
    lsblob = [l.name for l in lsblob]  # only return the filename from the blobs
    print(lsblob)

    return lsblob

# %%
@task()
def convert_to_parquet(filename: str, file: str, bucket) -> pd.DataFrame:
    """Convert csv file to parquet file
    (csv) -> parquet"""
    tmp_dir = "../tmp"
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    folder = filename.split("_")[0]
    trunc_fn = filename.split("_")[1].split(".")[0]
    output_file = f"../tmp/{trunc_fn}"
    outfile = open(output_file, "wb")
    outfile.write(file)
    outfile.close()

    if folder == "catch":
        dtype = {"user_id": int, "username": "string"}
        blob = bucket.blob(f"catch/{trunc_fn}.parquet")
    elif folder == "trip":
        dtype = {"user_id": int, "username": "string"}
        blob = bucket.blob(f"trip/{trunc_fn}.parquet")
    elif folder == "size":
        dtype = {"user_id": int, "username": "string"}
        blob = bucket.blob(f"size/{trunc_fn}.parquet")

    df = pd.read_csv(output_file, dtype=dtype)
    df.to_parquet(output_file.replace("csv", "parquet"))

    return trunc_fn, output_file, blob

# %%
@task()
def write_gcs(filename: str, file, blob) -> None:
    """Write parquet file to GCS
    (Dataframe) -> None"""
    
    with open(file, "rb") as myparquet:
        blob.upload_from_file(myparquet)

    print(f'Csv file size is {os.stat(f"../tmp/{filename}.csv").st_size}')
    print(f'Parquet file size is {os.stat(f"../tmp/{filename}.parquet").st_size}')
    os.remove(f"../tmp/{filename}.csv")  # delete/remove outfile
    os.remove(f"../tmp/{filename}.parquet")  # delete/remove outfile

# %%
@flow()
def unzip_blob(source_path: str, bucket) -> dict:
    """Unzip folder"""
    blob = bucket.blob(source_path)

    zipbytes = BytesIO(blob.download_as_string())

    lsfilename = []
    lsfile = []

    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, "r") as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)

                lsfilename.append(contentfilename)
                lsfile.append(contentfile)
    
    dictfile = dict(zip(lsfilename, lsfile))
    return dictfile

# %%
@flow(log_prints=False)
def process_gcs_blob() -> None:
    """Process blob in gcs bucket"""
    storage_client = storage.Client.from_service_account_json("../creds.json")
    bucket_name = "de_project_bucket"  # parameterize this
    bucket = storage_client.get_bucket(bucket_name)

    lsblob = read_gcs_bucket(bucket)

    for blob in lsblob:
        csvs = unzip_blob(blob, bucket)
        for k, v in csvs.items():
            print(k)
            parquet = convert_to_parquet(k, v, bucket)
            filename = parquet[0] #get file name without extension
            file = parquet[1]
            blob = parquet[2]
            write_gcs(filename, file, blob)
            # break
        # break

# %%
if __name__ == '__main__':
    process_gcs_blob()  


# %%



