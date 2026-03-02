import os
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from langchain_community.document_loaders import PyPDFLoader, Docx2txtLoader, TextLoader
from config.settings import Config

class IngestionAgent:
    def __init__(self):
        self.service = self._authenticate_drive()
    
    def _authenticate_drive(self):
        """Connect to Google Drive API"""
        try:
            creds = service_account.Credentials.from_service_account_file(
                Config.SERVICE_ACCOUNT_FILE,
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )
            return build('drive', 'v3', credentials=creds)
        except Exception as e:
            logging.error(f"⚠️ Drive Auth Failed: {e}")
            return None

    def download_new_files(self) -> list:
        """Download files from Drive to local folder"""
        if not self.service:
            return []
            
        print("🔍 Checking Google Drive for files...")
        os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
        
        try:
            results = self.service.files().list(
                q=f"'{Config.DRIVE_FOLDER_ID}' in parents and trashed=false",
                fields="files(id, name)"
            ).execute()
            
            items = results.get('files', [])
            downloaded_paths = []
            
            for item in items:
                file_path = os.path.join(Config.DOWNLOAD_DIR, item['name'])
                
                # Only download if we don't have it yet
                if not os.path.exists(file_path):
                    print(f"⬇️ Downloading: {item['name']}")
                    request = self.service.files().get_media(fileId=item['id'])
                    with open(file_path, "wb") as f:
                        downloader = MediaIoBaseDownload(f, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()
                    downloaded_paths.append(file_path)
                else:
                    # We still return the path so we can check if it's in the DB
                    downloaded_paths.append(file_path)
                    
            return downloaded_paths
            
        except Exception as e:
            logging.error(f"❌ Drive Error: {e}")
            return []

    def load_file_content(self, file_path: str) -> str:
        """Read text from PDF/DOCX"""
        try:
            if file_path.endswith('.pdf'):
                loader = PyPDFLoader(file_path)
            elif file_path.endswith('.docx'):
                loader = Docx2txtLoader(file_path)
            elif file_path.endswith('.txt'):
                loader = TextLoader(file_path)
            else:
                return ""
                
            docs = loader.load()
            return " ".join([d.page_content for d in docs])
            
        except Exception as e:
            logging.error(f"❌ Error reading {file_path}: {e}")
            return ""