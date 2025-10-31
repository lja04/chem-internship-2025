import os
import subprocess
import glob
import shutil
import csv
import re

def run_command(command, cwd=None):
    """Run a shell command and print output"""
    print(f"Running: {command}")
    result = subprocess.run(command, shell=True, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running command: {result.stderr}")
        return False
    else:
        print(result.stdout)
        return True

def find_db_files(base_dir):
    """Find database files in the base directory"""
    db_files = []
    for ext in ['*.db', '*.sqlite', '*.sqlite3']:
        db_files.extend(glob.glob(os.path.join(base_dir, ext)))
    
    # Also search in subdirectories for db files
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if any(file.endswith(ext) for ext in ['.db', '.sqlite', '.sqlite3']):
                db_files.append(os.path.join(root, file))
    
    return db_files

def get_clean_crystal_name(db_name):
    """Extract clean crystal name from database filename by removing extensions and suffixes"""
    # Remove file extension
    name = db_name.replace('.db', '').replace('.sqlite', '').replace('.sqlite3', '')
    
    # Remove common suffixes
    suffixes = ['_final', '_sohncke', '_cpk', '_packed', '_optimized', '_pack']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    
    return name

def organize_db_files(base_dir):
    """Move database files to their own folders and return folder list"""
    db_files = find_db_files(base_dir)
    crystal_folders = []
    
    for db_file in db_files:
        # Extract the clean crystal name for folder name
        db_name = os.path.basename(db_file)
        folder_name = get_clean_crystal_name(db_name)
        folder_path = os.path.join(base_dir, folder_name)
        
        # Create folder if it doesn't exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"Created folder: {folder_name}")
        
        # Move db file to folder if it's not already there
        if os.path.dirname(db_file) != folder_path:
            new_db_path = os.path.join(folder_path, db_name)
            try:
                shutil.move(db_file, new_db_path)
                print(f"Moved {db_name} to {folder_name}/")
            except FileNotFoundError:
                print(f"Warning: Could not find {db_name} to move. It might have been moved already.")
                # Check if it's already in the target location
                if os.path.exists(new_db_path):
                    print(f"File is already in target location: {new_db_path}")
        
        crystal_folders.append(folder_name)
    
    return crystal_folders

def load_observed_structures(csv_file):
    """Load the observed crystal structures from the CSV file"""
    observed_structures = {}
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                refcode = row['Refcode']
                csp_match = row['CSP_Match']
                observed_structures[refcode] = csp_match
        print(f"Loaded {len(observed_structures)} observed structures from {csv_file}")
    except FileNotFoundError:
        print(f"Warning: Could not find CSV file {csv_file}")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
    
    return observed_structures

def check_res_files(folder_path, refcode_variations, observed_structures):
    """Check if the RES files contain the observed crystal structure"""
    structure_dir = os.path.join(folder_path, "structure-files")
    
    if not os.path.exists(structure_dir):
        print(f"No structure-files directory found in {folder_path}")
        return False
    
    # Get all RES files in the structure directory
    res_files = glob.glob(os.path.join(structure_dir, "*.res"))
    
    if not res_files:
        print(f"No RES files found in {structure_dir}")
        return False
    
    # Check all possible refcode variations against the observed structures
    found_match = False
    for refcode in refcode_variations:
        if refcode in observed_structures:
            expected_res_file = os.path.join(structure_dir, f"{observed_structures[refcode]}.res")
            if os.path.exists(expected_res_file):
                print(f"✓ Found observed structure for {refcode}: {observed_structures[refcode]}.res")
                found_match = True
                break
            else:
                print(f"✗ Missing observed structure for {refcode}: {observed_structures[refcode]}.res")
    
    if not found_match:
        print(f"Available RES files: {[os.path.basename(f) for f in res_files]}")
        
        # Try to find any matching refcode in the CSV by checking all entries
        for refcode, csp_match in observed_structures.items():
            expected_res_file = os.path.join(structure_dir, f"{csp_match}.res")
            if os.path.exists(expected_res_file):
                print(f"✓ Found matching structure: {csp_match}.res (for refcode: {refcode})")
                found_match = True
                break
    
    return found_match

def get_refcode_variations(folder_name):
    """Generate all possible refcode variations from folder name"""
    # Generate variations
    variations = []
    
    # 1. Folder name in uppercase (e.g., "HOHLAR")
    variations.append(folder_name.upper())
    
    # 2. Folder name with all possible two-digit suffixes (00-99)
    for i in range(100):
        variations.append(folder_name.upper() + f"{i:02d}")
    
    # 3. If folder name ends with digits, try variations without those digits
    if re.search(r'\d+$', folder_name):
        base_without_numbers = re.sub(r'\d+$', '', folder_name)
        variations.append(base_without_numbers.upper())
        for i in range(100):
            variations.append(base_without_numbers.upper() + f"{i:02d}")
    
    # 4. Try to extract the base name if it contains numbers in the middle
    match = re.match(r'^([a-zA-Z]+)', folder_name)
    if match:
        base_alpha = match.group(1)
        variations.append(base_alpha.upper())
        for i in range(100):
            variations.append(base_alpha.upper() + f"{i:02d}")
    
    print(f"Generated {len(variations)} refcode variations for {folder_name}")
    return variations

def find_matching_refcode_in_csv(folder_name, observed_structures, base_dir):
    """Try to find a matching refcode in the CSV by checking RES files"""
    folder_path = os.path.join(base_dir, folder_name)
    structure_dir = os.path.join(folder_path, "structure-files")
    
    if not os.path.exists(structure_dir):
        return None
    
    # Get all RES files in the structure directory
    res_files = glob.glob(os.path.join(structure_dir, "*.res"))
    res_filenames = [os.path.basename(f).replace('.res', '') for f in res_files]
    
    # Check if any RES filename matches a CSP_Match in the CSV
    for refcode, csp_match in observed_structures.items():
        if csp_match in res_filenames:
            return refcode
    
    return None

def main():
    base_dir = "/lyceum/la3g22/summer-internship-2025/crystals"
    csv_file = "/scratch/la3g22/crystals-run-2.txt"
    
    # Check if base directory exists
    if not os.path.exists(base_dir):
        print(f"Error: Base directory {base_dir} does not exist!")
        return
    
    # Load observed structures from CSV
    observed_structures = load_observed_structures(csv_file)
    
    print("Organizing database files into folders...")
    crystal_folders = organize_db_files(base_dir)
    
    if not crystal_folders:
        print("No database files found!")
        return
    
    print(f"Found {len(crystal_folders)} crystal folders: {crystal_folders}")
    
    # First pass: run cspy-db dump on each folder
    for folder in crystal_folders:
        folder_path = os.path.join(base_dir, folder)
        print(f"\nProcessing {folder}...")
        
        # Find database files in this folder
        db_files = []
        for ext in ['.db', '.sqlite', '.sqlite3']:
            pattern = os.path.join(folder_path, f"*{ext}")
            db_files.extend(glob.glob(pattern))
        
        if not db_files:
            print(f"No database files found in {folder}, skipping...")
            continue
        
        # Use the first database file found
        db_file = os.path.basename(db_files[0])
        print(f"Using database file: {db_file}")
        
        # Run the cspy-db dump command
        cmd = f"cspy-db dump -f res -d {db_file} -e 8.0"
        success = run_command(cmd, cwd=folder_path)
        
        if not success:
            print(f"Failed to run cspy-db dump in {folder}")
    
    # Second pass: unzip structures in each folder
    for folder in crystal_folders:
        folder_path = os.path.join(base_dir, folder)
        structures_zip = os.path.join(folder_path, "structures.zip")
        
        if os.path.exists(structures_zip):
            print(f"\nUnzipping structures in {folder}...")
            
            # Create structure-files directory if it doesn't exist
            structure_dir = os.path.join(folder_path, "structure-files")
            if not os.path.exists(structure_dir):
                os.makedirs(structure_dir)
            
            # Unzip the structures
            cmd = f"unzip -o structures.zip -d structure-files"
            success = run_command(cmd, cwd=folder_path)
            
            if not success:
                print(f"Failed to unzip structures in {folder}")
        else:
            print(f"No structures.zip found in {folder}")
    
    # Third pass: validate RES files
    print("\n" + "="*50)
    print("VALIDATING RES FILES")
    print("="*50)
    
    validation_results = {}
    for folder in crystal_folders:
        folder_path = os.path.join(base_dir, folder)
        print(f"\nValidating {folder}...")
        
        # Generate all possible refcode variations
        refcode_variations = get_refcode_variations(folder)
        
        # Check if RES files contain the observed structure
        has_observed = check_res_files(folder_path, refcode_variations, observed_structures)
        
        # If still not found, try to find matching refcode by checking RES files against CSV
        if not has_observed:
            matching_refcode = find_matching_refcode_in_csv(folder, observed_structures, base_dir)
            if matching_refcode:
                print(f"✓ Found matching refcode in CSV: {matching_refcode}")
                has_observed = True
        
        # Use the folder name as the key for results
        validation_results[folder.upper()] = has_observed
    
    # Print summary
    print("\n" + "="*50)
    print("VALIDATION SUMMARY")
    print("="*50)
    
    valid_count = sum(1 for result in validation_results.values() if result)
    invalid_count = len(validation_results) - valid_count
    
    print(f"Structures with observed crystal: {valid_count}/{len(validation_results)}")
    
    if invalid_count > 0:
        print("\nStructures missing observed crystal:")
        for refcode, has_observed in validation_results.items():
            if not has_observed:
                print(f"  - {refcode}")

if __name__ == "__main__":
    main()