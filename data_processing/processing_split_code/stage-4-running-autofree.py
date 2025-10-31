import os
import shutil
import subprocess

completed_dirs = ['testing']
autofree_dir = 'AutoFree.py'
output_base_dir = 'testing/crystals-autofree-files'

def adding_autofree_python_file(completed_dirs, autofree_dir):
    for directory in completed_dirs:
        if not os.path.isdir(directory):
            print(f"Warning: Directory {directory} does not exist. Skipping.")
            continue

        for molecule in os.listdir(directory):
            molecule_path = os.path.join(directory, molecule)
            
            if not os.path.isdir(molecule_path):
                continue

            for crystal in os.listdir(molecule_path):
                crystal_path = os.path.join(molecule_path, crystal)
                
                if not os.path.isdir(crystal_path):
                    continue

                try:
                    shutil.copy(autofree_dir, crystal_path)
                    print(f'Successfully copied AutoFree.py into {crystal_path}')
                except Exception as e:
                    print(f"Error copying to {crystal_path}: {e}")

def count_autofree_runs(completed_dirs):
    """Count how many times AutoFree will need to run"""
    total = 0
    for directory in completed_dirs:
        if not os.path.isdir(directory):
            continue
        for molecule in os.listdir(directory):
            molecule_path = os.path.join(directory, molecule)
            if not os.path.isdir(molecule_path):
                continue
            for crystal in os.listdir(molecule_path):
                crystal_path = os.path.join(molecule_path, crystal)
                if os.path.isdir(crystal_path):
                    total += 1
    return total

def running_autofree(completed_dirs, total_runs):
    current_run = 0
    for directory in completed_dirs:
        if not os.path.isdir(directory):
            print(f"Warning: Directory {directory} does not exist. Skipping.")
            continue

        for molecule in os.listdir(directory):
            molecule_path = os.path.join(directory, molecule)
            
            if not os.path.isdir(molecule_path):
                continue

            for crystal in os.listdir(molecule_path):
                crystal_path = os.path.join(molecule_path, crystal)
                
                if not os.path.isdir(crystal_path):
                    continue

                current_run += 1
                try:
                    autofree_path = os.path.join(crystal_path, 'AutoFree.py')
                    output_file = os.path.join(crystal_path, f"{crystal}.out")
                    
                    if not os.path.isfile(autofree_path):
                        print(f"[{current_run}/{total_runs}] AutoFree.py not found in {crystal_path}")
                        continue
                    
                    print(f"\n[{current_run}/{total_runs}] Running AutoFree.py in {crystal_path}")
                    
                    # Command to run with warnings suppressed
                    cmd = (
                        "import warnings; "
                        "warnings.filterwarnings('ignore', category=DeprecationWarning); "
                        "warnings.filterwarnings('ignore', category=SyntaxWarning); "
                        f"exec(open('{autofree_path}').read())"
                    )
                    
                    with open(output_file, 'w') as f:
                        result = subprocess.run(['python', '-c', cmd], 
                                            cwd=crystal_path,
                                            stdout=f,
                                            stderr=subprocess.PIPE,
                                            text=True)
                    
                    if result.stderr:
                        error_lines = [line for line in result.stderr.split('\n') 
                                    if not ('DeprecationWarning' in line or 'SyntaxWarning' in line)]
                        actual_errors = '\n'.join(error_lines).strip()
                        if actual_errors:
                            print(f"[{current_run}/{total_runs}] Errors occurred:")
                            print(actual_errors)
                    
                    print(f"[{current_run}/{total_runs}] Completed - output saved to {output_file}")
                    
                except Exception as e:
                    print(f"[{current_run}/{total_runs}] Error running AutoFree.py: {e}")

def deleting_autofree_python_file(completed_dirs):
    for directory in completed_dirs:
        if not os.path.isdir(directory):
            print(f"Warning: Directory {directory} does not exist. Skipping.")
            continue

        for molecule in os.listdir(directory):
            molecule_path = os.path.join(directory, molecule)
            
            if not os.path.isdir(molecule_path):
                continue

            for crystal in os.listdir(molecule_path):
                crystal_path = os.path.join(molecule_path, crystal)
                
                if not os.path.isdir(crystal_path):
                    continue

                try:
                    autofree_path = os.path.join(crystal_path, 'AutoFree.py')
                    if os.path.exists(autofree_path):
                        os.remove(autofree_path)
                        print(f'Successfully deleted AutoFree.py from {crystal_path}')
                except Exception as e:
                    print(f"Error deleting AutoFree.py from {crystal_path}: {e}")

def copy_output_files(completed_dirs, output_base_dir):
    """Copy .out and .dos files to new directory structure"""
    for directory in completed_dirs:
        if not os.path.isdir(directory):
            print(f"Warning: Directory {directory} does not exist. Skipping.")
            continue

        for molecule in os.listdir(directory):
            molecule_path = os.path.join(directory, molecule)
            
            if not os.path.isdir(molecule_path):
                continue

            for crystal in os.listdir(molecule_path):
                crystal_path = os.path.join(molecule_path, crystal)
                
                if not os.path.isdir(crystal_path):
                    continue

                try:
                    # Create target directory structure
                    target_molecule_dir = os.path.join(output_base_dir, molecule)
                    target_crystal_dir = os.path.join(target_molecule_dir, crystal)
                    
                    os.makedirs(target_crystal_dir, exist_ok=True)
                    
                    # Copy .out file
                    out_file = os.path.join(crystal_path, f"{crystal}.out")
                    if os.path.exists(out_file):
                        shutil.copy2(out_file, target_crystal_dir)
                        print(f'Copied {out_file} to {target_crystal_dir}')
                    
                    # Copy .dos files (assuming multiple .dos files may exist)
                    for file in os.listdir(crystal_path):
                        if file.endswith('.dos'):
                            dos_file = os.path.join(crystal_path, file)
                            shutil.copy2(dos_file, target_crystal_dir)
                            print(f'Copied {dos_file} to {target_crystal_dir}')
                            
                except Exception as e:
                    print(f"Error copying files from {crystal_path}: {e}")

def main():
    print('\nSTAGE 1: Counting AutoFree runs...')
    total_runs = count_autofree_runs(completed_dirs)
    print(f'Found {total_runs} directories to process')
    print('\n ****************************************** STAGE 1 COMPLETED ******************************************')

    print('\nSTAGE 2: Copying AutoFree Files')
    adding_autofree_python_file(completed_dirs, autofree_dir)
    print('\n ****************************************** STAGE 2 COMPLETED ******************************************')
    
    print('\nSTAGE 3: Running AutoFree.py')
    running_autofree(completed_dirs, total_runs)
    print('\n ****************************************** STAGE 3 COMPLETED ******************************************')

    print('\nSTAGE 4: Deleting AutoFree Files')
    deleting_autofree_python_file(completed_dirs)
    print('\n ****************************************** STAGE 4 COMPLETED ******************************************')

    print('\nSTAGE 5: Copying output files to organized directory')
    copy_output_files(completed_dirs, output_base_dir)
    print('\n ****************************************** STAGE 5 COMPLETED ******************************************')

if __name__ == "__main__":
    main()