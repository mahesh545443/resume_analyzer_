import subprocess
import sys
import os
import time

def main():
    # CRITICAL: Get the directory where launcher.py actually sits
    base_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_script = os.path.join(base_dir, "pipeline.py")
    app_script = os.path.join(base_dir, "app.py")

    print("==================================================")
    print("   🚀 ANALYTICS AVENUE: AUTOMATED STARTUP SYSTEM")
    print("==================================================")

    # --- STEP 1: RUN THE HEAVY PIPELINE ---
    print("\n[1/2] 🧠 Checking & Processing Resumes...")
    
    try:
        # Run pipeline.py using the absolute path
        subprocess.run([sys.executable, pipeline_script], check=True)
        
    except subprocess.CalledProcessError:
        print("\n❌ CRITICAL ERROR: The pipeline failed.")
        return
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
        return

    # --- STEP 2: LAUNCH THE CHAT APP ---
    print("\n" + "="*50)
    print("✅ PIPELINE COMPLETE. STARTING UI...")
    print("="*50 + "\n")
    
    time.sleep(1) 
    
    try:
        # Launch Streamlit using the absolute path to app.py
        subprocess.run([sys.executable, "-m", "streamlit", "run", app_script])
    except KeyboardInterrupt:
        print("\n👋 App closed.")

if __name__ == "__main__":
    main()