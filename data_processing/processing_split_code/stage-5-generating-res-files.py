import os
import shutil
import glob
import zipfile
import subprocess

# Define the source directories
source_dirs = [
    "testing"
]

# Define the database source directory
db_source_dir = "crystal-databases/first-crystals"

# Define the target base directory
target_base = "testing/working-crystals"

# Create the target directory if it doesn't exist
os.makedirs(target_base, exist_ok=True)

# First, collect all crystal names from the run directories
crystal_names = set()

for source_dir in source_dirs:
    if not os.path.exists(source_dir):
        print(f"Warning: Source directory not found: {source_dir}")
        continue
    
    try:
        # Get all crystal directories in the source directory
        for item in os.listdir(source_dir):
            full_path = os.path.join(source_dir, item)
            if os.path.isdir(full_path):
                crystal_names.add(item)
    except Exception as e:
        print(f"Error reading directory {source_dir}: {e}")

print(f"Found {len(crystal_names)} unique crystal names")

# Now search for matching database files
db_patterns = [
    "{name}_final.db",
    "{name}_sohncke.db",
    "{name}_cpk.db",
    "{name}.db",  # Generic fallback
    "{name}_pack.db"
]

processed_count = 0

for crystal_name in crystal_names:
    found = False
    target_dir = os.path.join(target_base, crystal_name)
    
    # Try each possible pattern
    for pattern in db_patterns:
        db_filename = pattern.format(name=crystal_name)
        source_db_path = os.path.join(db_source_dir, db_filename)
        
        if os.path.exists(source_db_path):
            os.makedirs(target_dir, exist_ok=True)
            target_db_path = os.path.join(target_dir, db_filename)
            
            try:
                # Copy the database file
                shutil.copy2(source_db_path, target_db_path)
                print(f"Copied: {source_db_path} -> {target_db_path}")
                
                # Change to the target directory
                os.chdir(target_dir)
                
                # Run the cspy-db dump command
                cmd = f"cspy-db dump -f res -d {db_filename} -e 8.0"
                print(f"Executing: {cmd}")
                subprocess.run(cmd, shell=True, check=True)
                
                # Create structure-files directory
                structure_dir = os.path.join(target_dir, "structure-files")
                os.makedirs(structure_dir, exist_ok=True)
                
                # Unzip the structures.zip file
                zip_path = os.path.join(target_dir, "structures.zip")
                if os.path.exists(zip_path):
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(structure_dir)
                    print(f"Unzipped structures to: {structure_dir}")
                    
                    # Optionally remove the zip file after extraction
                    os.remove(zip_path)
                else:
                    print(f"Warning: structures.zip not found in {target_dir}")
                
                processed_count += 1
                found = True
                break  # Stop after first match
            
            except subprocess.CalledProcessError as e:
                print(f"Error executing command for {crystal_name}: {e}")
            except Exception as e:
                print(f"Error processing {crystal_name}: {e}")
    
    if not found:
        print(f"Warning: No matching DB file found for {crystal_name} in {db_source_dir}")

print(f"\nOperation completed. Processed {processed_count} database files out of {len(crystal_names)} crystals.")