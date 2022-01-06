# Compose a data ingestion pipeline
import json
import os
from yt_audio.utils import yid_to_url
from yt_audio.download import download_audio
from yt_audio.process import process_audio


def ingest(data_file: str, sample_rate: int, output_dir: str):
    """
    Data ingestion includes the following steps
    1. Each id is converted to YouTube URL
    2. Each entry is downloaded
    3. New download is split
    4. New split is resampled to target sample rate
    """
    with open(data_file, "r") as f:
        dataset = json.load(f)

    raw_audio_dir = os.path.join(output_dir, "raw")
    processed_audio_dir = os.path.join(output_dir, "processed")

    os.makedirs(raw_audio_dir, exist_ok=True)
    os.makedirs(processed_audio_dir, exist_ok=True)

    for index , _id in enumerate(dataset["id"]):
        _url = yid_to_url(_id)
        _label = dataset["label"][index]
        _start = dataset["start"][index]
        _end = dataset["end"][index]

        file_path, stop_process = download_audio(_url, raw_audio_dir)

        if file_path is not None:
            chunk_file_path = process_audio(file_path, processed_audio_dir, _label,
                                            sample_rate, _start, _end)
            print(f"Created file {chunk_file_path}")
        else:
            print(f"Skipping id {_id} - could not be downloaded from YouTube")
        
        if stop_process:
            print("Stopping process now")
            break
