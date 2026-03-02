import subprocess
import sys
import os
import time

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_script = os.path.join(base_dir, "pipeline.py")
    app_script = os.path.join(base_dir, "app.py")

    print("==================================================")
    print("   🚀 ANALYTICS AVENUE: AUTOMATED STARTUP SYSTEM")
    print("==================================================")

    # ✅ Read secrets from .streamlit/secrets.toml and inject into environment
    env = os.environ.copy()

    try:
        secrets_path = os.path.join(base_dir, ".streamlit", "secrets.toml")
        if os.path.exists(secrets_path):
            try:
                import tomllib  # Python 3.11+
            except ImportError:
                import tomli as tomllib
            with open(secrets_path, "rb") as f:
                secrets = tomllib.load(f)
            # Inject all secrets as environment variables
            for k, v in secrets.items():
                if isinstance(v, str):
                    env[k] = v
            print("✅ Secrets loaded into environment.")
        else:
            print("⚠️ No secrets.toml found — relying on existing env vars.")
    except Exception as e:
        print(f"⚠️ Could not load secrets: {e}")

    # --- STEP 1: RUN THE HEAVY PIPELINE ---
    print("\n[1/2] 🧠 Checking & Processing Resumes...")

    try:
        # ✅ Pass env with injected secrets to pipeline subprocess
        subprocess.run([sys.executable, pipeline_script], check=True, env=env)

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
        subprocess.run([sys.executable, "-m", "streamlit", "run", app_script], env=env)
    except KeyboardInterrupt:
        print("\n👋 App closed.")

if __name__ == "__main__":
    main()