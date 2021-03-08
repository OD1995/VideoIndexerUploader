import logging
from moviepy.editor import VideoFileClip, AudioFileClip
from azure.storage.blob import BlockBlobService
from MyClasses import VideoIndexer
from azure.storage.blob import ContainerPermissions
from datetime import datetime, timedelta
import os
from urllib.parse import unquote
import requests
import pyodbc

def initial_function(
    fileURL,
    containerInput
):
    ## Ensure the container is right
    if containerInput != "videoindexer-files":
        return f"Container is `{containerInput}`, so file ignored"

    ## Create bbs 
    bbs = BlockBlobService(connection_string=os.getenv("fsevideosConnectionString"))
    ## Get SAS URL
    sasURL = get_SAS_URL(
        fileURL=fileURL,
        block_blob_service=bbs,
        container=containerInput
    ).replace(" ","%20")
    logging.info(f"sasURL: {sasURL}")
    try:
        ## Get file in moviepy object
        if fileURL.lower().endswith(".mp4"):
            logging.info("it's a video file")
            clip = VideoFileClip(sasURL)
        elif fileURL.lower().endswith(".mp3") | fileURL.lower().endswith(".wav"):
            logging.info("it's an audio file")
            clip = AudioFileClip(sasURL)
        else:
            raise ValueError(f"File is neither MP4 nor MP3/WAV: {fileURL}")
    except OSError:
        ## If we get an error from reading in the URL, we're going to retry
        logging.info("error from reading in, let's retry")
        return "retry"
    ## Decide action based on file's length
    if clip.duration > 7200:
        ## If longer than 2 hours, add to queue (outside of function)
        return "split"
    else:
        ## If shorter than 2 hours, upload to Video Indexer
        logging.info("file is shorter than 2 hours")
        fileID = upload_file(
            fileURL=fileURL,
            bbs=bbs,
            containerInput=containerInput
        )
        ## Insert row into VideoIndexerIDs
        Q = f"""
        INSERT INTO VideoIndexerIDs (VideoID,FileURL)
        VALUES ('{fileID}','{fileURL}')
        """
        run_sql_query(Q)
        return f"File uploaded, ID: {fileID}"

def upload_file(
    fileURL,
    bbs,
    containerInput
):
    ## Create VideoIndexer object
    vi = VideoIndexer(
        vi_subscription_key=os.getenv("SUBSCRIPTION_KEY"),
        vi_location=os.getenv('LOCATION'),
        vi_account_id=os.getenv('ACCOUNT_ID'),
        block_blob_service=bbs,
        container_source=containerInput
    )
    ## Give it the same name as in blob storage (80 char limit)
    fileName = unquote(fileURL.split("/")[-1])[:80]
    ## Upload to Video Indexer
    r = vi.upload_to_video_indexer(
        video_url=fileURL,
        video_name=fileName  # identifier for video in Video Indexer platform, must be unique during indexing time
    )
    ## Return assigned video ID
    return r.json()['id']


def get_SAS_URL(fileURL,
                block_blob_service,
                container):

    sasTokenRead = block_blob_service.generate_container_shared_access_signature(
    container_name=container,
    permission=ContainerPermissions.READ,
    expiry=datetime.utcnow() + timedelta(days=1)
    )
    return f"{fileURL}?{sasTokenRead}"

def run_sql_query(query):

    ## Get information used to create connection string
    username = 'matt.shepherd'
    password = os.getenv("sqlPassword")
    driver = '{ODBC Driver 17 for SQL Server}'
    server = os.getenv("sqlServer")
    database = 'AzureCognitive'
    ## Create connection string
    connectionString = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    logging.info(f"connectionString: {connectionString}")
    ## Execute query
    with pyodbc.connect(connectionString) as conn:
        with conn.cursor() as cursor:
            logging.info("About to execute 'INSERT' query")
            cursor.execute(query)
            logging.info("'INSERT' query executed")