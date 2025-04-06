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

    # Analyze URL for container + blob name
    parsed = urlparse(blob_url)
    path_parts = parsed.path.lstrip('/').split('/')
    container_name = path_parts[0]
    blob_name = '/'.join(path_parts[1:])

    # Storage connection
    connection_string = os.environ["AzureWebJobsStorage"]

    # Create temporary files
    tmp_dir = f"/tmp/{uuid.uuid4()}"
    os.makedirs(tmp_dir, exist_ok=True)

    # Save the video locally using SDK
    local_input_path = os.path.join(tmp_dir, "input.mp4")

    # Download blob (on-demand)
    blob_client = BlobClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name,
        blob_name=blob_name
    )
    with open(local_input_path, "wb") as f:
        download_stream = blob_client.download_blob()
        f.write(download_stream.readall())


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
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        logging.info(result.stdout.decode())
        print(result.stdout.decode(), flush=True)

        if result.returncode != 0:
            logging.error("❌ ffmpeg failed.")
            print("❌ ffmpeg failed.", flush=True)
            return
        else:
            logging.info("✅ Το βίντεο κόπηκε επιτυχώς σε segments.")
            print("✅ Το βίντεο κόπηκε επιτυχώς σε segments.", flush=True)
    except Exception as e:
        logging.error(f"❌ Σφάλμα κατά την εκτέλεση του ffmpeg: {e}")
        print(f"❌ Σφάλμα κατά την εκτέλεση του ffmpeg: {e}", flush=True)
        return

    # Έλεγχος αν δημιουργήθηκαν αρχεία
    if not os.listdir(segments_folder):
        logging.error("❌ Κανένα segment δεν δημιουργήθηκε από το ffmpeg.")
        print("❌ Κανένα segment δεν δημιουργήθηκε από το ffmpeg.", flush=True)
        return


    # Upload των segments
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("processed-videos")

    for filename in os.listdir(segments_folder):
        segment_path = os.path.join(segments_folder, filename)
        blob_name = filename

        with open(segment_path, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)
            logging.info(f"📤 Ανεβάστηκε: processed-videos/{blob_name}")
            print(f"📤 Ανεβάστηκε: processed-videos/{blob_name}", flush=True)

    # Καθαρισμός: Διαγραφή όλων των προσωρινών αρχείων και φακέλων
    try:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
            logging.info(f"🧹 Διαγράφηκε προσωρινός φάκελος: {tmp_dir}")
            print(f"🧹 Διαγράφηκε προσωρινός φάκελος: {tmp_dir}", flush=True)
    except Exception as cleanup_error:
        logging.warning(f"⚠️ Αποτυχία καθαρισμού: {cleanup_error}")
        print(f"⚠️ Αποτυχία καθαρισμού: {cleanup_error}", flush=True)