import os
import requests
import time
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from datetime import datetime, timedelta

# Set the scopes and credentials
SCOPES = ['https://www.googleapis.com/auth/photoslibrary.readonly']
CLIENT_SECRET_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
PHOTO_DOWNLOAD_FOLDER = 'downloaded_photos'
VIDEO_DOWNLOAD_FOLDER = 'downloaded_videos'

# Create download folders if they don't exist
os.makedirs(PHOTO_DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(VIDEO_DOWNLOAD_FOLDER, exist_ok=True)


def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="media_downloads"
    )


def authenticate():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    return creds


def load_downloaded_items():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM downloaded_items")
    downloaded_items = [row[0] for row in cursor.fetchall()]
    conn.close()
    return downloaded_items


def save_downloaded_item(item_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT IGNORE INTO downloaded_items (id) VALUES (%s)", (item_id,))
    conn.commit()
    conn.close()


def get_unique_filename(folder, filename):
    base, ext = os.path.splitext(filename)
    counter = 1
    new_filename = filename
    while os.path.exists(os.path.join(folder, new_filename)):
        new_filename = f"{base}_{counter}{ext}"
        counter += 1
    return new_filename


def download_media(item):
    media_type = item['mimeType']
    filename = item['filename']
    folder = VIDEO_DOWNLOAD_FOLDER if 'video' in media_type else PHOTO_DOWNLOAD_FOLDER
    file_path = os.path.join(folder, get_unique_filename(folder, filename))
    download_url = item['baseUrl'] + '=d'

    print(f'Downloading {filename} from {download_url}')
    img_data = requests.get(download_url).content

    with open(file_path, 'wb') as f:
        f.write(img_data)
    print(f'Saved {filename} to {file_path}')
    save_downloaded_item(item['id'])


def download_media_items(creds):
    access_token = creds.token
    headers = {'Authorization': f'Bearer {access_token}'}
    next_page_token = None
    total_downloads = len(load_downloaded_items())
    minute_downloads = 0
    start_time = time.time()
    daily_reset_time = datetime.now() + timedelta(days=1)

    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            url = 'https://photoslibrary.googleapis.com/v1/mediaItems?pageSize=100'
            if next_page_token:
                url += f"&pageToken={next_page_token}"

            response = requests.get(url, headers=headers)
            response.raise_for_status()
            items = response.json().get('mediaItems', [])
            downloaded_items = load_downloaded_items()
            items_to_download = [
                item for item in items if item['id'] not in downloaded_items]

            for item in items_to_download:
                executor.submit(download_media, item)
                total_downloads += 1
                minute_downloads += 1

                if minute_downloads >= 1000:
                    elapsed_time = time.time() - start_time
                    if elapsed_time < 60:
                        time.sleep(60 - elapsed_time)
                    start_time = time.time()
                    minute_downloads = 0

                if total_downloads >= 10000:
                    print("Reached daily limit of 10,000 downloads. Stopping.")
                    return

            if datetime.now() >= daily_reset_time:
                total_downloads = 0
                daily_reset_time = datetime.now() + timedelta(days=1)

            next_page_token = response.json().get('nextPageToken')
            if not next_page_token:
                break


if __name__ == '__main__':
    creds = authenticate()
    download_media_items(creds)
