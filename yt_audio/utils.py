import os
import uuid


def yid_to_url(yid: str) -> str:
    return f"https://www.youtube.com/watch?v=yid"


def get_unique_file_name_in(output_dir: str, extension: str = "wav"):
    def _unique_file_name():
        return f"{uuid.uuid4()}.{extension}"

    file_name = _unique_file_name()

    while file_name in os.listdir(output_dir):
        file_name = _unique_file_name()

    return file_name
