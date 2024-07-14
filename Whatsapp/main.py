'''
Whatsapp Newsletter Bot
https://forobeta.com/members/len0.320511/
'''


import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import base64
import requests
import json
import threading
from dotenv import load_dotenv


load_dotenv(".env")
WHAPI_TOKEN = os.getenv("WHAPI_TOKEN")
CHANNEL_NAME = os.getenv("CHANNEL_NAME")
FILES_PATH = os.getenv("FILES_PATH")


MESSAGES = []


class FileChangeHandler(FileSystemEventHandler):
    def on_created(self, event):
        print(f"New file detected: '{event.src_path}'")
        if not event.is_directory:
            if event.src_path.endswith(('.txt', '.csv')):
                threading.Timer(10.0, process_text_file, args=[event.src_path]).start()
            elif event.src_path.endswith(('.png', '.jpg', '.jpeg', '.gif')):
                threading.Timer(10.0, send_image_to_newsletter_channel, args=[event.src_path]).start()


def process_text_file(file_path):
    read_file(file_path)
    for message in MESSAGES:
        send_message_to_newsletter_channel(message)
    MESSAGES.clear()


def monitor_folder(path):
    event_handler = FileChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


def read_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        current_message = ""
        for line in file:
            if len(current_message) + len(line) > 65536:
                MESSAGES.append(current_message)
                current_message = line
            else:
                current_message += line
        if current_message:
            MESSAGES.append(current_message)


def get_newsletter_channel(name):
    url = "https://gate.whapi.cloud/newsletters"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {WHAPI_TOKEN}",
    }
    try:
        response = requests.get(url, headers=headers)
        newsletters = response.json()["newsletters"]
        for newsletter in newsletters:
            if newsletter["name"] == name:
                return newsletter["id"]
        return ""
    except Exception as e:
        return ""


CHANNEL_ID = get_newsletter_channel(CHANNEL_NAME)


def send_message_to_newsletter_channel(message):
    url = "https://gate.whapi.cloud/messages/text"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {WHAPI_TOKEN}",
        "content-type": "application/json",
    }
    data = {"typing_time": 0, "to": CHANNEL_ID, "body": message}
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print("Message sent")
        return response.text
    except Exception as e:
        print(e)
        return ""


def send_image_to_newsletter_channel(image_path):
    url = "https://gate.whapi.cloud/messages/image"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {WHAPI_TOKEN}", 
    }
    
    mime_type = "image/png"
    extension = os.path.splitext(image_path)[1].lower()
    if extension == ".jpg" or extension == ".jpeg":
        mime_type = "image/jpeg"
    elif extension == ".gif":
        mime_type = "image/gif"
    
    with open(image_path, "rb") as image_file:
        base64_image = base64.b64encode(image_file.read()).decode('utf-8')
    
    data = {
        "media": f"data:{mime_type};name={os.path.basename(image_path)};base64,{base64_image}",
        "to": CHANNEL_ID,
    }
    
    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            print("Image sent")
        else:
            print(f"Failed to send image. Status code: {response.status_code}")
        return response.text
    except Exception as e:
        print(f"An error occurred: {e}")
        return ""


if __name__ == "__main__":
    if CHANNEL_ID == "":
        print("Channel not found")
        exit(1)
    
    print(f"Monitoring folder: '{FILES_PATH}'")
    FILES_PATH = os.getenv("FILES_PATH")
    monitor_folder(FILES_PATH)