import os
import shutil
from pathlib import Path

# Define directories
source_dirs = [
    "testing"
]

error_dirs = [
    "testing-with-errors/imaginary-error"
]

unrun_dirs = [
    "testing-with-errors/unrun-dmacrys"
]

# Target directories
working_crystals = "testing/working-crystals"
imaginary_dir = os.path.join(working_crystals, "imaginary-crystals")
unrun_dir = os.path.join(working_crystals, "un-run-crystals")

# Create target directories if they don't exist
os.makedirs(imaginary_dir, exist_ok=True)
os.makedirs(unrun_dir, exist_ok=True)

def find_crystal_qr_codes(directories):
    """Find all crystal QR codes in the given directories"""
    crystal_qrs = {}
    for directory in directories:
        if not os.path.exists(directory):
            print(f"Warning: Directory not found: {directory}")
            continue
        
        for root, dirs, files in os.walk(directory):
            for dir_name in dirs:
                if "-QR-" in dir_name:
                    base_name = dir_name.split("-QR-")[0]
                    crystal_qrs[dir_name] = {
                        "path": os.path.join(root, dir_name),
                        "base_name": base_name
                    }
    return crystal_qrs

# Find all crystal QR codes in each type of directory
normal_crystals = find_crystal_qr_codes(source_dirs)
imaginary_crystals = find_crystal_qr_codes(error_dirs)
unrun_crystals = find_crystal_qr_codes(unrun_dirs)

print(f"Found {len(normal_crystals)} normal crystals")
print(f"Found {len(imaginary_crystals)} imaginary crystals")
print(f"Found {len(unrun_crystals)} unrun crystals")

# Process the .res files
for crystal_qr, info in {**normal_crystals, **imaginary_crystals, **unrun_crystals}.items():
    base_name = info["base_name"]
    res_file = os.path.join(working_crystals, base_name, "structure-files", f"{crystal_qr}.res")
    
    if not os.path.exists(res_file):
        print(f"Warning: .res file not found for {crystal_qr}")
        continue
    
    # Determine where to move/delete based on origin
    if crystal_qr in normal_crystals:
        # Delete .res file for normal crystals
        os.remove(res_file)
        print(f"Deleted .res file for normal crystal: {crystal_qr}")
    elif crystal_qr in imaginary_crystals:
        # Move to imaginary-crystals
        dest_dir = os.path.join(imaginary_dir, base_name)
        os.makedirs(dest_dir, exist_ok=True)
        shutil.move(res_file, os.path.join(dest_dir, f"{crystal_qr}.res"))
        print(f"Moved to imaginary-crystals: {crystal_qr}")
    else:
        # Move to un-run-crystals
        dest_dir = os.path.join(unrun_dir, base_name)
        os.makedirs(dest_dir, exist_ok=True)
        shutil.move(res_file, os.path.join(dest_dir, f"{crystal_qr}.res"))
        print(f"Moved to un-run-crystals: {crystal_qr}")

print(f"Found {len(normal_crystals)} normal crystals")
print(f"Found {len(imaginary_crystals)} imaginary crystals")
print(f"Found {len(unrun_crystals)} unrun crystals")

print("Operation completed.")