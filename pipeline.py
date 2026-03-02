import os
import time
import logging
from tqdm import tqdm

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Imports
from agents.ingestion_agent import IngestionAgent
from agents.extraction_agent import ExtractionAgent
from core.database import db
from core.vector_db import VectorDB
from config.settings import Config

def run_pipeline():
    print("🚀 STARTING RESUME PIPELINE...")
    
    # 1. Setup Agents
    ingestor = IngestionAgent()
    extractor = ExtractionAgent()
    vector_db = VectorDB()
    
    # 2. Download from Drive
    print("\n📂 Step 1: Downloading files from Drive...")
    ingestor.download_new_files()
    
    # 3. Get all local files
    os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
    all_files = [f for f in os.listdir(Config.DOWNLOAD_DIR) if f.endswith(('.pdf', '.docx', '.txt'))]
    
    print(f"📂 Found {len(all_files)} resumes in local folder.")
    
    # 4. Processing Loop
    print("\n🧠 Step 2: Extracting & Embedding...")
    
    processed_count = 0
    errors = 0
    
    for filename in tqdm(all_files, desc="Processing Resumes", unit="file"):
        try:
            # Check DB first (Skip if already done)
            if db.check_processed(filename):
                continue
                
            # CRITICAL FIX: Use ABSOLUTE PATH so app.py can always find the file
            file_path = os.path.abspath(os.path.join(Config.DOWNLOAD_DIR, filename))
            
            # Read Text
            text = ingestor.load_file_content(file_path)
            if not text:
                continue
                
            # Extract Data (AI)
            data = extractor.extract(text)
            if not data:
                errors += 1
                continue
                
            # Calculate Experience
            real_exp = extractor.calculate_experience(data.work_history)
            
            # CRITICAL FIX: Determine Category (Needed for your "Category-wise count" prompt)
            # We use the AI-extracted domains or skills to pick a main category
            category = data.domains[0] if data.domains else "General"

            # Prepare Data with all necessary fields for the QueryAgent
            profile_data = {
                "filename": filename,
                "name": data.full_name,
                "email": data.email,
                "phone": data.phone,
                "total_experience": real_exp,
                "skills": ", ".join(data.skills),
                "domains": ", ".join(data.domains),
                "category": category,      # NEW: Added for counts
                "file_path": file_path,    # ABSOLUTE path
                "full_text": text          # Needed for Domain search
            }
            
            # Save to SQL
            db.save_candidate_full(profile_data, data.work_history, data.projects)
            
            # Save to Vector DB
            vector_db.add_resume(text, {"name": data.full_name, "filename": filename})
            
            processed_count += 1
            
            # Rate Limit: 20s is high for Groq; 2-5s is usually safe for 8B models
            time.sleep(2.0) 
            
        except Exception as e:
            logging.error(f"Failed on {filename}: {e}")
            errors += 1

    print("\n" + "="*50)
    print(f"✅ PIPELINE FINISHED!")
    print(f"📊 Processed: {processed_count}")
    print(f"⚠️ Errors: {errors}")
    print("="*50)

if __name__ == "__main__":
    run_pipeline()