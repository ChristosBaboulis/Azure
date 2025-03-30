import os
import subprocess
import logging
import uuid
from azure.storage.blob import BlobServiceClient
import azure.functions as func

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="videos/{name}.mp4",
                  connection="highwayfootagestorage_STORAGE") 
def split_video(myblob: func.InputStream):
    logging.info(f"📥 Blob trigger ενεργοποιήθηκε για: {myblob.name} ({myblob.length} bytes)")

    # Storage connection
    connection_string = os.environ["AzureWebJobsStorage"]

    # Δημιουργία προσωρινών φακέλων
    tmp_dir = f"/tmp/{uuid.uuid4()}"
    os.makedirs(tmp_dir, exist_ok=True)

    # Αποθήκευση του βίντεο τοπικά
    local_input_path = os.path.join(tmp_dir, "input.mp4")
    with open(local_input_path, "wb") as f:
        f.write(myblob.read())

    # Φάκελος για τα segments
    segments_folder = os.path.join(tmp_dir, "segments")
    os.makedirs(segments_folder, exist_ok=True)

    # Επιλογή ffmpeg path ανάλογα με το περιβάλλον
    if os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") == "Development":
        ffmpeg_path = "ffmpeg"
    else:
        ffmpeg_path = os.path.join(os.path.dirname(__file__), "bin", "ffmpeg", "ffmpeg")

    # Ρύθμιση ονόματος για τα output segments
    output_pattern = os.path.join(segments_folder, "segment_%03d.mp4")
    segment_duration = 120  # 2 λεπτά

    # ffmpeg command
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

    # Εκτέλεση ffmpeg
    try:
        subprocess.run(cmd, check=True)
        logging.info("✅ Το βίντεο κόπηκε επιτυχώς σε segments.")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Σφάλμα με ffmpeg: {e}")
        return

    # Upload των segments
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("processed-videos")

    for filename in os.listdir(segments_folder):
        segment_path = os.path.join(segments_folder, filename)
        blob_name = f"{myblob.name.replace('videos/', '').replace('.mp4', '')}/{filename}"

        with open(segment_path, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)
            logging.info(f"📤 Ανεβάστηκε: processed-videos/{blob_name}")
