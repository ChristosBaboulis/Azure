import os
import subprocess
import logging
import uuid
from azure.storage.blob import BlobServiceClient, BlobClient
import azure.functions as func
import shutil
from urllib.parse import urlparse

app = func.FunctionApp()

@app.event_grid_trigger(arg_name="event")
def split_video(event: func.EventGridEvent):
    # Take URL of Blob
    data = event.get_json()
    blob_url = data["url"]
    logging.info(f"Event received for: {blob_url}")
    print(f"Event received for: {blob_url}", flush=True)

    # Analyze URL for Container + Blob Name
    parsed = urlparse(blob_url)
    path_parts = parsed.path.lstrip('/').split('/')
    container_name = path_parts[0]
    blob_name = '/'.join(path_parts[1:])

    # Storage Connection
    connection_string = os.environ["AzureWebJobsStorage"]

    # Create Temporary Files
    tmp_dir = f"/tmp/{uuid.uuid4()}"
    os.makedirs(tmp_dir, exist_ok=True)

    # Save Video Locally Using SDK
    local_input_path = os.path.join(tmp_dir, "input.mp4")

    # Download Blob (On-Demand)
    blob_client = BlobClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name,
        blob_name=blob_name
    )
    with open(local_input_path, "wb") as f:
        download_stream = blob_client.download_blob()
        f.write(download_stream.readall())


    # Temp Folder of Segments
    segments_folder = os.path.join(tmp_dir, "segments")
    os.makedirs(segments_folder, exist_ok=True)

    # Choose ffmpeg Path Depending on Local Env or Portal Env
    if os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") == "Development":
        ffmpeg_path = "ffmpeg"
    else:
        ffmpeg_path = os.path.join(os.path.dirname(__file__), "bin", "ffmpeg", "ffmpeg")

    # Output Segments Name Creation
    output_pattern = os.path.join(segments_folder, "segment_%03d.mp4")
    segment_duration = 120  # 2 λεπτά

    # ffmpeg Command
    cmd = [
        ffmpeg_path,
        "-i", local_input_path,
        "-c", "copy",
        "-map", "0",
        "-segment_time", str(segment_duration),
        "-f", "segment",
        "-reset_timestamps", "1",
        output_pattern
    ]

    # ffmpeg Execution
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        logging.info(result.stdout.decode())
        print(result.stdout.decode(), flush=True)

        if result.returncode != 0:
            logging.error("❌ ffmpeg failed.")
            print("❌ ffmpeg failed.", flush=True)
            return
        else:
            logging.info("✅ Video split into segments successfully.")
            print("✅ Video split into segments successfully.", flush=True)
    except Exception as e:
        logging.error(f"❌ Error while executing ffmpeg: {e}")
        print(f"❌ Error while executing ffmpeg: {e}", flush=True)
        return

    # Check if Files are Created
    if not os.listdir(segments_folder):
        logging.error("❌ No segment was created from ffmpeg.")
        print("❌ No segment was created from ffmpeg.", flush=True)
        return


    # Upload Segments
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("processed-videos")

    for filename in os.listdir(segments_folder):
        segment_path = os.path.join(segments_folder, filename)
        blob_name = filename

        with open(segment_path, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)
            logging.info(f"📤 Uploaded: processed-videos/{blob_name}")
            print(f"📤 Uploaded: processed-videos/{blob_name}", flush=True)

    # Delete temp files/folders
    try:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
            logging.info(f"🧹 Temp folder deleted: {tmp_dir}")
            print(f"🧹 Temp folder deleted: {tmp_dir}", flush=True)
    except Exception as cleanup_error:
        logging.warning(f"⚠️ Failed to delete: {cleanup_error}")
        print(f"⚠️ Failed to delete: {cleanup_error}", flush=True)