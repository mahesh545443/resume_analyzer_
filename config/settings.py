import os
from pathlib import Path

class Config:
    # === PATHS ===
    # Gets the absolute path of the 'resume_agent_v2' folder
    BASE_DIR = Path(__file__).parent.parent.absolute()
    
    # Path to your Google Drive Service Account JSON
    # VERIFY THIS PATH IS CORRECT on your machine:
    SERVICE_ACCOUNT_FILE = r"C:\Users\User\Downloads\gen-lang-client-0450530409-c38d5de3f755.json"
    
    # Where to save files and database
    DATA_DIR = os.path.join(BASE_DIR, "data")
    DOWNLOAD_DIR = os.path.join(DATA_DIR, "resumes")
    DB_PATH = os.path.join(DATA_DIR, "resume_metadata.db")
    VECTORSTORE_PATH = os.path.join(DATA_DIR, "vectorstore.pkl")
    EMBEDDINGS_PATH = os.path.join(DATA_DIR, "embeddings.pkl")
    
    # === GOOGLE DRIVE ===
    # The folder ID where resumes are stored in Drive
    DRIVE_FOLDER_ID = "1bSH_SI2-PSCpWKqDOO87LHIaK5PeMqMf4deRig7N-0NfoydGgsWfrwJojX-LTsQzKjRDkieP"
    
    # === AI MODELS ===
    # Using your Groq Key
    GROQ_API_KEY = "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"
    
    MODEL_NAME = "llama-3.1-8b-instant"
    EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"
    
    # === OPTIMIZATIONS ===
    CHUNK_SIZE = 500
    CHUNK_OVERLAP = 50
    
    # Automatically create directories if they don't exist
    @staticmethod
    def setup():
        os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
        os.makedirs(Config.DATA_DIR, exist_ok=True)

# Run setup immediately when imported
Config.setup()