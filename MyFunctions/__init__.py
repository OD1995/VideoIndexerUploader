import logging
from moviepy.editor import VideoFileClip, AudioFileClip
from azure.storage.blob import BlockBlobService
from MyClasses import VideoIndexer
from azure.storage.blob import ContainerPermissions
from datetime import datetime, timedelta
import os
from urllib.parse import unquote
import requests

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
    ## Get file in moviepy object
    if fileURL.lower().endswith(".mp4"):
        clip = VideoFileClip(sasURL)
    elif fileURL.lower().endswith(".mp3") | fileURL.lower().endswith(".wav"):
        clip = AudioFileClip(sasURL)
    else:
        raise ValueError(f"File is neither MP4 nor MP3/WAV: {fileURL}")
    ## Decide action based on file's length
    if clip.duration > 7200:
        ## If longer than 2 hours, add to queue (outside of function)
        return "split"
    else:
        ## If shorter than 2 hours, upload to Video Indexer
        fileID = upload_file(
            fileURL=fileURL,
            bbs=bbs,
            containerInput=containerInput
        )
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
    ## Give it the same name as in blob storage
    fileName = unquote(fileURL.split("/")[-1])
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