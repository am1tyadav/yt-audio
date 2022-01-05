import os
import librosa
import soundfile
from yt_audio.utils import get_unique_file_name_in


def process_audio(file_path: str, output_dir: str, label: str, target_sr: int,
                  start_time: float, end_time: float) -> str:
    target_dir = os.path.join(output_dir, label)
    os.makedirs(target_dir, exist_ok=True)

    sr = librosa.get_samplerate(file_path)
    audio, _ = librosa.load(file_path, sr=sr, mono=True)

    num_samples = len(audio)
    start_sample = min(int(start_time * sr), num_samples - 1)
    end_sample = min(int(end_time * sr), num_samples - 1)
    audio_chunk = audio[start_sample: end_sample]

    if sr != target_sr:
        audio_chunk = librosa.resample(audio_chunk, orig_sr=sr, target_sr=target_sr)

    new_file_path = get_unique_file_name_in(target_dir, extension="wav")
    soundfile.write(new_file_path, audio_chunk, samplerate=target_sr)
    return new_file_path
