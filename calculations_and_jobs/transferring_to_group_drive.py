import os
import shutil
import zipfile
from pathlib import Path

crystal_base_directory = "crystals"
group_drive_directory = "summer-crystals"

# Initialize counters
file_stats = {
    'total_checked': 0,
    'total_moved': 0,
    'total_remaining': 0
}

def dmaout_completed_test(filepath):
    try:
        with open(filepath, 'r') as file:
            content = file.read()
            
            # Successful completion indicators
            if "Total run time" in content:
                return True
            if "Symmetry Adapted" in content:
                return True
            
            # Specific error message that indicates completion (though with errors)
            if "ERROR - Axes set up error" in content and "check order of neighbours in MOLX" in content:
                return True
            
            return False
        
    except Exception as e:
        print(f"Error checking {filepath}: {str(e)}")
        return False

def cleaning_dma_files(base_directory, group_drive_directory):
    for crystal_folder in os.listdir(base_directory):
        crystal_path = os.path.join(base_directory, crystal_folder)

        if not os.path.isdir(crystal_path):
            continue

        structure_path = os.path.join(crystal_path, "structure-files")
        if not os.path.exists(structure_path):
            continue
        
        for res_folder in os.listdir(structure_path):
            res_path = os.path.join(structure_path, res_folder)
            
            if not os.path.isdir(res_path):
                continue
            
            dest_dir = os.path.join(group_drive_directory, crystal_folder, "structure-files", res_folder)
            os.makedirs(dest_dir, exist_ok=True)
            
            file_list = os.listdir(res_path)
            matched_files = []
            
            for file in file_list:
                file_stats['total_checked'] += 1
                if file.endswith(".dmain"):
                    base_name = file[:-6]
                    if f"{base_name}.dmaout" in file_list:
                        matched_files.append(base_name)
            
            for base_name in matched_files:
                dmain_file = os.path.join(res_path, f"{base_name}.dmain")
                dmaout_file = os.path.join(res_path, f"{base_name}.dmaout")
                
                if dmaout_completed_test(dmaout_file):
                    shutil.move(dmain_file, os.path.join(dest_dir, f"{base_name}.dmain"))
                    shutil.move(dmaout_file, os.path.join(dest_dir, f"{base_name}.dmaout"))
                    file_stats['total_moved'] += 2
                    print(f"Moved to {dest_dir}")
                else:
                    file_stats['total_remaining'] += 2

def cleaning_res_folders(base_directory, group_drive_directory):
    for crystal_folder in os.listdir(base_directory):
        crystal_path = os.path.join(base_directory, crystal_folder)

        if not os.path.isdir(crystal_path):
            continue

        structure_path = os.path.join(crystal_path, "structure-files")
        if not os.path.exists(structure_path):
            continue
        
        for res_folder in os.listdir(structure_path):
            res_path = os.path.join(structure_path, res_folder)
            
            if not os.path.isdir(res_path):
                continue
            
            dest_dir = os.path.join(group_drive_directory, crystal_folder, "structure-files", res_folder)
            os.makedirs(dest_dir, exist_ok=True)
            
            file_list = os.listdir(res_path)
            if any(f.endswith(('.dmain', '.dmaout')) for f in file_list):
                file_stats['total_remaining'] += len(file_list)
                continue
            
            fort_files = [f for f in file_list if f.startswith('fort.')]
            if not fort_files:
                file_stats['total_remaining'] += len(file_list)
                continue
            
            zip_name = f"{res_folder}_fort_files.zip"
            zip_path = os.path.join(dest_dir, zip_name)
            
            try:
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file in fort_files:
                        file_path = os.path.join(res_path, file)
                        zipf.write(file_path, arcname=file)
                        file_stats['total_checked'] += 1
                
                print(f"Created zip at {zip_path}")
                
                if os.path.exists(zip_path):
                    for file in fort_files:
                        os.remove(os.path.join(res_path, file))
                    
                    if not os.listdir(res_path):
                        os.rmdir(res_path)
                        print(f"Deleted empty folder: {res_path}")
                    
            except Exception as e:
                print(f"Error processing {res_path}: {str(e)}")

def print_file_stats():
    print("\nFILE PROCESSING SUMMARY:")
    print(f"{'Total files checked:':<20} {file_stats['total_checked']}")
    print(f"{'Total files moved:':<20} {file_stats['total_moved']}")
    print(f"{'Total files remaining:':<20} {file_stats['total_remaining']}")

if __name__ == "__main__":
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ START OF SCRIPT ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    print("\nSTAGE 1: Processing completed DMA files")
    cleaning_dma_files(crystal_base_directory, group_drive_directory)
    
    print("\nSTAGE 2: Zipping remaining Fortran files")
    cleaning_res_folders(crystal_base_directory, group_drive_directory)
    
    print_file_stats()
    
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ END OF SCRIPT ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")