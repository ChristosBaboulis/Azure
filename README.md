# Azure Function - Video Segment Splitter

This Azure Function is the first component of the Vehicle Speed Calculation project.  

This repo contains an Azure Function that splits uploaded videos into 2-minute segments using the ffmpeg library. It is designed to be triggered automatically when a video is uploaded to a specific Azure Blob Storage container.

## Architecture Overview

1. **Azure Blob Storage**  
   * Stores the uploaded full-length videos.  
   * Triggers the Azure Function via Blob Trigger on upload.

2. **Azure Function - split_video**  
   * Triggered automatically when a new video is uploaded.  
   * Uses ffmpeg to split the video into segments of 2 minutes each.  
   * Stores the resulting video segments in a designated output container.

## Features

* Splits video files into equal 2-minute segments  
* Uses `ffmpeg` command-line utility  
* Triggered automatically on video upload  
* Stores output segments in blob storage

## Tech Stack

* Python 3.10+  
* Azure Functions (Blob Trigger)  
* Azure Blob Storage  
* ffmpeg
