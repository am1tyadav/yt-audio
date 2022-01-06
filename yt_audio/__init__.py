# Compose a data ingestion pipeline
import json
import os
import warnings
from yt_audio.utils import yid_to_url
from yt_audio.download import download_audio
from yt_audio.process import process_audio


warnings.simplefilter("ignore")


def ingest(data_file: str, sample_rate: int, output_dir: str, reset: bool = False):
    """
    Data ingestion includes the following steps
    1. Each id is converted to YouTube URL
    2. Each entry is downloaded
    3. New download is split
    4. New split is resampled to target sample rate
    """
    with open(data_file, "r") as f:
        dataset = json.load(f)

    num_examples = len(dataset["id"])

    if "processed" not in dataset or reset:
        dataset["processed"] = [False] * num_examples
        dataset["file_path"] = [""] * num_examples

    raw_audio_dir = os.path.join(output_dir, "raw")
    processed_audio_dir = os.path.join(output_dir, "processed")

    os.makedirs(raw_audio_dir, exist_ok=True)
    os.makedirs(processed_audio_dir, exist_ok=True)

    for index , _id in enumerate(dataset["id"]):
        if dataset["processed"][index]:
            continue

        _url = yid_to_url(_id)
        _label = dataset["label"][index]
        _start = dataset["start"][index]
        _end = dataset["end"][index]

        file_path, stop_process = download_audio(_url, raw_audio_dir)

        if file_path is not None:
            chunk_file_path = process_audio(file_path, processed_audio_dir, _label,
                                            sample_rate, _start, _end)
            print(f"{index}/{num_examples} Created file {chunk_file_path}")
            dataset["file_path"][index] = chunk_file_path
        else:
            print(f"Skipping id {_id} - could not be downloaded from YouTube")
        
        if stop_process:
            print("Stopping process now")
            break

        # Irrespective of the result, file was still processed
        dataset["processed"][index] = True

    # Overwrite updated file
    with open(data_file, "w") as f:
        print("Saving updated file")
        json.dump(dataset, f)
