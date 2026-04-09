import subprocess
import sys
import os

# -------------------------
# Configuration
# -------------------------
scripts_folder = "D:\Data-Engineering-Project\scripts"  # Path to scripts folder

pipeline_steps = [
    ("Fetch data from Yahoo Finance", "ingestion.py"),
    ("Create the db engine","db.py"),
    ("Load CSVs to Postgres", "load_to_postgres.py"),
    ("Create dimension tables","dim_tickers.py"),
    ("Clean & Transform data", "transform.py"),
    #("Auto refresh the dashboard in PowerBI","auto_refresh.py"),
]

# -------------------------
# Helper function to run a script
# -------------------------
def run_script(script_name):
    script_path = os.path.join(scripts_folder, script_name)
    print(f"\n=== Running: {script_name} ===")
    
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Error in {script_name}")
        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)
        sys.exit(1)
    
    print(f"✅ {script_name} completed successfully")
    return result.stdout

# -------------------------
# Run pipeline
# -------------------------
if __name__ == "__main__":
    print("Starting ETL pipeline...\n")
    for step_name, script_file in pipeline_steps:
        print(f"Step: {step_name}")
        run_script(script_file)
    print("\n🎉 ETL pipeline completed successfully!")