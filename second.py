import os
import requests
import mysql.connector
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# Set the scopes and credentials
SCOPES = ['https://www.googleapis.com/auth/photoslibrary.readonly']
CLIENT_SECRET_FILE = 'credentials.json'  # Path to your client secret JSON file
TOKEN_FILE = 'token.json'  # Path to save the token
# Folder to save the downloaded videos
VIDEO_DOWNLOAD_FOLDER = 'downloaded_videos'

# Create download folder if it doesn't exist
os.makedirs(VIDEO_DOWNLOAD_FOLDER, exist_ok=True)

# MySQL database connection settings
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = ''
DB_NAME = 'media_downloader'

# Connect to MySQL database


def connect_to_db():
    connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    return connection

# Authenticate and create the credentials


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

# Check if an item has already been downloaded


def is_downloaded(item_id, db_conn):
    cursor = db_conn.cursor()
    cursor.execute("SELECT id FROM downloaded_items WHERE id = %s", (item_id,))
    result = cursor.fetchone()
    cursor.close()
    return result is not None

# Save a downloaded item ID to the MySQL database


def save_downloaded_item(item_id, db_conn):
    cursor = db_conn.cursor()
    cursor.execute("INSERT INTO downloaded_items (id) VALUES (%s)", (item_id,))
    db_conn.commit()
    cursor.close()

# Check if a file with the same name exists and create a new name if necessary


def get_unique_filename(folder, filename):
    base, ext = os.path.splitext(filename)
    counter = 1
    new_filename = filename
    while os.path.exists(os.path.join(folder, new_filename)):
        new_filename = f"{base}_{counter}{ext}"
        counter += 1
    return new_filename

# Download a single video


def download_video(item, db_conn):
    media_type = item['mimeType']  # Get the media type
    filename = item['filename']

    # Ensure it's a video
    if 'video' not in media_type:
        return

    # Determine the folder for video downloads
    folder = VIDEO_DOWNLOAD_FOLDER
    file_path = os.path.join(folder, get_unique_filename(folder, filename))
    download_url = item['baseUrl']  # No '=d' for videos

    print(f'Downloading {filename} from {download_url}')
    try:
        # Download the video
        response = requests.get(download_url, stream=True)
        response.raise_for_status()

        with open(file_path, 'wb') as f:
            # Write in chunks of 1 MB
            for chunk in response.iter_content(1024 * 1024):
                f.write(chunk)
        print(f'Saved {filename} to {file_path}')
        # Save the downloaded item's ID
        save_downloaded_item(item['id'], db_conn)
    except Exception as e:
        print(f"Failed to download {filename}: {str(e)}")

# Download only videos using the REST API


def download_videos_only(creds, db_conn):
    access_token = creds.token  # Get the access token
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    next_page_token = None

    # Use 5 threads for downloading
    with ThreadPoolExecutor(max_workers=2) as executor:
        while True:
            url = 'https://photoslibrary.googleapis.com/v1/mediaItems?pageSize=100'
            if next_page_token:
                url += f"&pageToken={next_page_token}"

            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an error for bad responses
            items = response.json().get('mediaItems', [])

            # Filter out non-video items and already downloaded videos
            items_to_download = [
                item for item in items if 'video' in item['mimeType'] and not is_downloaded(item['id'], db_conn)
            ]

            # Submit each download task to the executor
            for item in items_to_download:
                executor.submit(download_video, item, db_conn)

            next_page_token = response.json().get('nextPageToken')
            if not next_page_token:
                break


if __name__ == '__main__':
    db_conn = connect_to_db()  # Connect to MySQL
    creds = authenticate()  # Authenticate with Google Photos
    download_videos_only(creds, db_conn)  # Download only videos
    db_conn.close()  # Close the MySQL connection when done
