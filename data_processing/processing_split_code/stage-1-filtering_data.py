import os
import shutil

base_crystal_directory = 'crystals'

def initial_filtering_stage(base_crystal_directory):
    completed_list = []
    troubleshoot_dmaout_list = []
    dmain_list = []

    for crystal_folder in os.listdir(base_crystal_directory):
        crystal_path = os.path.join(base_crystal_directory, crystal_folder)

        if not os.path.isdir(crystal_path):
            print("Skipping non-directory:", crystal_path)
            continue

        structure_path = os.path.join(crystal_path, "structure-files")

        if not os.path.exists(structure_path):
            print("Skipping missing structure files directory:", structure_path)
            continue
        
        for crystal_folder in os.listdir(structure_path):
            crystal_folder_path = os.path.join(structure_path, crystal_folder)
        
            for dma_file in os.listdir(crystal_folder_path):
                dma_file_path = os.path.join(crystal_folder_path, dma_file)

                if os.path.isfile(dma_file_path) and dma_file.endswith(".dmaout"):
                    with open(dma_file_path, 'r') as file:
                        lines = [line.strip() for line in file.readlines() if line.strip()]
                    
                    if not lines:
                        continue
                    
                    last_line = lines[-1]
                    
                    if "Symmetry Adapted" in last_line or "Total run time" in last_line:
                        print(f"Found completed file: {dma_file}")
                        completed_list.append(dma_file_path)
                    else:
                        print(f"Found incomplete file: {dma_file}")
                        troubleshoot_dmaout_list.append(dma_file_path)

                elif os.path.isfile(dma_file_path) and dma_file.endswith(".dmain"):
                    dmain_list.append(dma_file_path)

                else:
                    try:
                        print(f"Deleting non-dma file: {dma_file_path}")
                        os.remove(dma_file_path)
                    except OSError as e:
                        print(f"Error deleting {dma_file_path}: {e.strerror}")

    final_completed = completed_list.copy()
    final_troubleshoot = []
    
    for file in troubleshoot_dmaout_list:
        try:
            with open(file, 'r') as f:
                content = f.read()
                if "Zone Centre Phonon Frequencies" not in content:
                    print(f"Adding file without phonon data to completed list: {file}")
                    final_completed.append(file)
                else:
                    final_troubleshoot.append(file)
        except:
            final_troubleshoot.append(file)
    
    return final_completed, final_troubleshoot, dmain_list

def imaginary_phonon_filter(dmaout_file_list):
    imaginary_phonon_list = []
    real_phonon_list = []

    for dmaout_file in dmaout_file_list:
        with open(dmaout_file, 'r') as file:
            lines = file.readlines()

        start_index = -1
        for i, line in enumerate(lines):
            if "Zone Centre Phonon Frequencies" in line:
                start_index = i + 3
                break
        
        if start_index == -1:
            real_phonon_list.append(dmaout_file)
            continue
        
        frequencies = []
        for line in lines[start_index:]:
            if not line.strip():
                break
            parts = line.split()
            if len(parts) >= 1:
                try:
                    freq = float(parts[0])
                    frequencies.append(freq)
                except ValueError:
                    continue
        
        has_imaginary = any(freq < -0.0001 for freq in frequencies)
        
        if has_imaginary:
            imaginary_phonon_list.append(dmaout_file)
        else:
            real_phonon_list.append(dmaout_file)
    
    return real_phonon_list, imaginary_phonon_list

def preparing_for_autofree(real_phonon_list, imaginary_phonon_list, dmain_list, troubleshoot_dmaout_list=None):
    """
    Move files to appropriate directories based on their status:
    - Real phonon files -> crystals-temp
    - Imaginary phonon files -> crystals-with-errors/imaginary-error
    - Troubleshooting files -> crystals-with-errors/troubleshooting
    """
    

    real_dest_base = "testing"
    imaginary_dest_base = "testing/imaginary-error"
    troubleshoot_dest_base = "testing/troubleshooting"


    os.makedirs(real_dest_base, exist_ok=True)
    os.makedirs(imaginary_dest_base, exist_ok=True)
    os.makedirs(troubleshoot_dest_base, exist_ok=True)

    directories_to_clean = set()
    
    def process_file(file_list, destination_base, file_type):
        moved_files = []
        missing_dmain = []
        
        for dmaout_path in file_list:
            directories_to_clean.add(os.path.dirname(dmaout_path))
            
            dmain_path = dmaout_path.replace(".dmaout", ".dmain")
            
            if dmain_path not in dmain_list:
                missing_dmain.append(dmaout_path)
                continue
            
            path_parts = dmaout_path.split(os.sep)
            crystal_name = path_parts[-4]
            subfolder = path_parts[-2]
            filename = path_parts[-1]
            
            dest_path = os.path.join(
                destination_base,
                crystal_name,
                subfolder,
                filename
            )
            
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            try:
                shutil.move(dmaout_path, dest_path)
                dmain_dest = dest_path.replace(".dmaout", ".dmain")
                shutil.move(dmain_path, dmain_dest)
                moved_files.append((dmaout_path, dest_path))
            except Exception as e:
                print(f"Error moving {dmaout_path}: {str(e)}")
        
        return moved_files, missing_dmain
    
    print("\nProcessing real phonon files...")
    real_moved, real_missing = process_file(real_phonon_list, real_dest_base, "real")
    
    print("\nProcessing imaginary phonon files...")
    imaginary_moved, imaginary_missing = process_file(imaginary_phonon_list, imaginary_dest_base, "imaginary")
    
    troubleshoot_moved, troubleshoot_missing = [], []
    if troubleshoot_dmaout_list:
        print("\nProcessing troubleshooting files...")
        troubleshoot_moved, troubleshoot_missing = process_file(troubleshoot_dmaout_list, troubleshoot_dest_base, "troubleshooting")
    
    print("\nCleaning up empty directories...")
    for directory in directories_to_clean:
        try:
            if not os.listdir(directory):
                os.removedirs(directory)
                print(f"Removed empty directory: {directory}")
        except OSError as e:
            print(f"Error removing directory {directory}: {e}")
    
    print("\n=== Summary ===")
    print(f"Successfully moved {len(real_moved)} real phonon file pairs")
    print(f"Files with missing dmain: {len(real_missing)}")
    print(f"Successfully moved {len(imaginary_moved)} imaginary phonon file pairs")
    print(f"Files with missing dmain: {len(imaginary_missing)}")
    if troubleshoot_dmaout_list:
        print(f"Successfully moved {len(troubleshoot_moved)} troubleshooting file pairs")
        print(f"Files with missing dmain: {len(troubleshoot_missing)}")
    
    return {
        "real_moved": real_moved,
        "real_missing_dmain": real_missing,
        "imaginary_moved": imaginary_moved,
        "imaginary_missing_dmain": imaginary_missing,
        "troubleshoot_moved": troubleshoot_moved,
        "troubleshoot_missing_dmain": troubleshoot_missing
    }

completed_dmaout_list, troubleshoot_dmaout_list, dmain_list = initial_filtering_stage(base_crystal_directory)
real, imaginary = imaginary_phonon_filter(completed_dmaout_list)
result = preparing_for_autofree(real, imaginary, dmain_list, troubleshoot_dmaout_list)