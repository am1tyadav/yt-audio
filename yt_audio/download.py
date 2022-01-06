import os
from pytube import YouTube
from uuid import uuid4
from yt_audio.utils import get_unique_file_name_in


def _get_best_stream_index(streams, required_mime_type="audio/mp4"):
    _rates = []
    _index = -1

    for st in streams:
        if st.mime_type == required_mime_type:
            _abr = st.abr.replace("kbps", "")
            _rates.append(int(_abr))

    _rates = sorted(_rates)
    if len(_rates) > 0:
        _index = len(_rates) - 1
    return _index


def download_audio(url: str, output_dir: str) -> (str, bool):
    """
    Downloads audio from YouTube url and exports to output_dir
    Returns the final output path is successfully downloaded
    """
    output_path = None
    stop_process = False

    try:
        streams = YouTube(url=url).streams.filter(only_audio=True, file_extension="mp4")
        index = _get_best_stream_index(streams)

        if index != -1:
            filename = get_unique_file_name_in(output_dir, extension="mp4")
            streams[index].download(output_path=output_dir, filename=filename)
            output_path = os.path.join(output_dir, filename)
        else:
            print("Could not download audio stream")
    except Exception as e:
        print(e)
        print("Something went wrong")
        output_path = None

        if str(e) == "HTTP Error 429: Too Many Requests":
            stop_process = True

    return output_path, stop_process
