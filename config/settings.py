import os
from pathlib import Path

# Load .env file for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _get_secret(key: str) -> str:
    """Get secret from env var first, then Streamlit secrets."""
    # Try env var first
    value = os.getenv(key)
    if value:
        return value

    # Try reading from .streamlit/secrets.toml directly (works in pipeline/scripts)
    try:
        secrets_path = Path(__file__).parent.parent / ".streamlit" / "secrets.toml"
        if secrets_path.exists():
            try:
                import tomllib  # Python 3.11+
            except ImportError:
                import tomli as tomllib  # fallback for older Python
            with open(secrets_path, "rb") as f:
                secrets = tomllib.load(f)
            val = secrets.get(key)
            if val:
                return val
    except Exception:
        pass

    # Try Streamlit secrets (works when running via streamlit run)
    try:
        import streamlit as st
        val = st.secrets.get(key)
        if val:
            return val
    except Exception:
        pass

    return None


class Config:
    # === PATHS ===
    BASE_DIR = Path(__file__).parent.parent.absolute()

    SERVICE_ACCOUNT_FILE = r"C:\Users\User\Downloads\gen-lang-client-0450530409-c38d5de3f755.json"

    DATA_DIR = os.path.join(BASE_DIR, "data")
    DOWNLOAD_DIR = os.path.join(DATA_DIR, "resumes")
    DB_PATH = os.path.join(DATA_DIR, "resume_metadata.db")
    VECTORSTORE_PATH = os.path.join(DATA_DIR, "vectorstore.pkl")
    EMBEDDINGS_PATH = os.path.join(DATA_DIR, "embeddings.pkl")

    # === GOOGLE DRIVE ===
    DRIVE_FOLDER_ID = "1bSH_SI2-PSCpWKqDOO87LHIaK5PeMqMf4deRig7N-0NfoydGgsWfrwJojX-LTsQzKjRDkieP"

    # === AI MODELS ===
    @staticmethod
    def get_groq_key():
        key = _get_secret("GROQ_API_KEY")
        if not key:
            raise ValueError("GROQ_API_KEY is not set in environment or Streamlit Secrets!")
        return key

    MODEL_NAME = "llama-3.1-8b-instant"
    EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"

    CHUNK_SIZE = 500
    CHUNK_OVERLAP = 50

    @staticmethod
    def setup():
        os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
        os.makedirs(Config.DATA_DIR, exist_ok=True)


Config.setup()
