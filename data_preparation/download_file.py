import os
import re
import time
import gzip
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _build_session(max_retries: int) -> requests.Session:
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _extract_confirm_token(response: requests.Response):
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            return value
    return None


def _extract_filename(response: requests.Response, default_name: str) -> str:
    content_disp = response.headers.get("content-disposition", "")
    match = re.search(r'filename="(?P<name>.+)"', content_disp)
    if match:
        return match.group("name")
    return default_name


def _extract_gdrive_file_id(url: str) -> str:
    """
    Extract the Google Drive file ID from typical share or download URLs.
    Supports:
      - https://drive.google.com/file/d/<FILE_ID>/view?usp=sharing
      - https://drive.google.com/uc?id=<FILE_ID>&export=download
    """
    # /d/<id>/ pattern
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if match:
        return match.group(1)
    # id=<id> pattern
    match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    if match:
        return match.group(1)
    return ""


def _stream_download(response: requests.Response, output_path: str):
    total_size = int(response.headers.get("content-length", 0))
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "wb") as f:
        downloaded = 0
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    print(f"\rProgress: {percent:.1f}% ({downloaded}/{total_size} bytes)", end='', flush=True)
    if total_size > 0:
        print()  # newline after progress


def _verify_gzip(output_path: str):
    if not output_path.endswith(".gz"):
        return

    print("Verifying gzip file integrity...")
    try:
        with gzip.open(output_path, "rb") as f:
            f.read(1024)
        print("File integrity verified successfully!")
    except Exception as e:
        print(f"File verification failed: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)
        raise Exception("Downloaded file is corrupted")


def download_google_drive_file(url, output_dir=".", max_retries=5, timeout=30):
    """
    Download a public Google Drive file, handling the confirmation token for large files.

    Args:
        url: Public Google Drive share link or direct download link.
        output_dir: Directory to place the downloaded file.
        max_retries: Maximum number of retry attempts for connection issues.
        timeout: Timeout for each request in seconds.
    """
    file_id = _extract_gdrive_file_id(url)
    if not file_id:
        raise ValueError(f"Could not extract Google Drive file id from URL: {url}")

    print(f"Downloading Google Drive file: {url}")
    session = _build_session(max_retries)

    base_url = "https://drive.google.com/uc"
    params = {"export": "download", "id": file_id}

    attempt = 0
    while attempt < max_retries:
        try:
            response = session.get(base_url, params=params, stream=True, timeout=timeout)
            token = _extract_confirm_token(response)
            if token:
                # Needs confirmation for large files
                response.close()
                params["confirm"] = token
                response = session.get(base_url, params=params, stream=True, timeout=timeout)

            response.raise_for_status()
            filename = _extract_filename(response, default_name=f"{file_id}.bin")
            output_path = os.path.join(output_dir, filename)

            _stream_download(response, output_path)
            print("Download completed!")
            _verify_gzip(output_path)
            print(f"File saved to: {os.path.abspath(output_path)}")
            return output_path

        except (requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            attempt += 1
            print(f"\nConnection error (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                wait_time = 2 ** attempt
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Download failed.")
                raise
        except requests.exceptions.RequestException as e:
            print(f"\nError downloading file: {e}")
            raise
        except Exception as e:
            print(f"\nError saving file: {e}")
            raise

    raise Exception("Download failed after all retry attempts")


def download_google_drive_files(urls, output_dir=".", max_retries=5, timeout=30):
    """
    Download multiple public Google Drive files sequentially.
    """
    paths = []
    for url in urls:
        path = download_google_drive_file(
            url=url,
            output_dir=output_dir,
            max_retries=max_retries,
            timeout=timeout,
        )
        paths.append(path)
    return paths


if __name__ == "__main__":
    # Replace these with your public Google Drive file URLs
    urls = [
        "https://drive.google.com/file/d/FILE_ID_1/view?usp=sharing",
        "https://drive.google.com/file/d/FILE_ID_2/view?usp=sharing",
    ]
    output_directory = "/Users/dannytram/MYSELF/MLOps_K6/final_project/Product-Recommender-Image-Search-ML-System/data_preparation/dataset"
    download_google_drive_files(urls, output_dir=output_directory)
