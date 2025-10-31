# Author: Leo Arogundade
# Date: August 2025
# Description: This script processes crystal structure data, filtering based on calculation completion and phonon modes

# Importing necessary libraries
import os
import shutil
from collections import defaultdict
import subprocess
import glob
import zipfile
from pathlib import Path
import re
import pandas as pd
import csv

# Directory that contains all of the raw crystal structure data after AutoLD.py was run
base_directory = '/lyceum/la3g22/summer-internship-2025/crystals'

# Directory to store the results
results_directory = '/scratch/la3g22/test_2/raw_results'

# Specifying the run number for directory naming
run_number = '1'

# Directory containing the AutoFree.py script
autofree_directory = '/scratch/la3g22/code/core-code/AutoFree.py'

# Directory to store the output from AutoFree.py
autofree_output_directory = '/scratch/la3g22/test_2/autofree_results'

# Directory that contains organised crystal structure data
autofree_completed_directories = f'{results_directory}/crystals-run-{run_number}'

# Path to the .db files directory
db_source_directory = '/scratch/la3g22/crystal-databases/second-crystals'

# Path to where to save the crystal .db files that have been processed
working_crystals_directory = '/scratch/la3g22/test_2/working_crystal_dbs'

# Directory that all crystal structure csv files, which are generated from the cspy-db command
initial_info_directory = '/scratch/la3g22/initial-crystal-info'

# Output directory for organised structure csvs
output_directory_for_organised_structure_csvs = '/scratch/la3g22/test_2/organised_crystal_structure_csvs'

# Directory for all the report files
report_directory = '/scratch/la3g22/test_2/reports'

# Directory for the final ranks file
final_ranks_directory = '/scratch/la3g22/code/core-code/final_ranks_with_Sohncke.csv'

def initial_filter(base_directory):

    '''
    This function filters through the raw crystal structure data and organises the files into three lists:
    1. complete_list - contains all of the dmaout files that have completed successfully and have phonon data
    2. errored_dmaout_list - contains all of the dmaout files that have errored during the calculation and have phonon data
    3. dmain_list - contains all of the dmain files that have been generated

    INPUT: base_directory - the directory that contains all of the raw crystal structure data
    OUTPUT: final_complete_list - list of all the successfully completed dmaout files with phonon data
            final_errored - list of all the errored dmaout files with phonon data
            dmain_list - list of all the dmain files
    '''
    # Creating filtered lists
    complete_list = []
    errored_dmaout_list = []
    dmain_list = []

    
    for crystal_folder in os.listdir(base_directory):
        crystal_path = os.path.join(base_directory, crystal_folder)

        # Making sure the directory exists
        if not os.path.isdir(crystal_path):
            print(f'The following directory does not exist and will be skipped:', crystal_path)
            continue
        
        # Selecting the structure-files folder in each crystal directory
        crystal_structure_path = os.path.join(crystal_path, "structure-files")

        # Making sure the directory exists
        if not os.path.exists(crystal_structure_path):
            print(f'The following crystal does not have a folder named structure-files:', crystal_path)
            continue
        
        for sub_crystal_folder in os.listdir(crystal_structure_path):
            sub_crystal_folder_path = os.path.join(crystal_structure_path, sub_crystal_folder)

            for file in os.listdir(sub_crystal_folder_path):
                file_path = os.path.join(sub_crystal_folder_path, file)

                # Storing all the lines of the file in "lines" for the dmaout files
                if os.path.isfile(file_path) and file.endswith("dmaout"):
                    with open(file_path, 'r') as info:
                        lines = [line.strip() for line in info.readlines() if line.strip()]

                    if not lines:
                        continue

                    # Getting the information for the last line of the file    
                    last_line = lines[-1]

                    # Organising the files into their corresponding lists
                    if "Symmetry Adapted" in last_line or "Total run time" in last_line:
                        print(f'Found fully complete file: {file}')
                        complete_list.append(file_path)
                    else:
                        print(f"Found an errored file: {file}")
                        errored_dmaout_list.append(file_path)
                
                # Adding all of the dmain files into their corresponding list
                elif os.path.isfile(file_path) and file.endswith(".dmain"):
                    dmain_list.append(file_path)

                else:
                    try:
                        print(f"Removing following file due to it not meeting the correct criteria: {file_path}")
                        os.remove(file_path)
                    except OSError as e:
                        print(f"An error occured while trying to delete the following file: {file_path}\nError message: {e.strerror}")

    final_complete_list = complete_list.copy()

    final_errored = []

    # Checking the errored files for phonon data

    for file in errored_dmaout_list:
        try:
            with open(file, 'r') as f:
                content = f.read()
                if "Zone Centre Phonon Frequencies" not in content:
                    print(f'Adding file without phonon data to complete list: {file}')
                    final_complete_list.append(file)
                else:
                    final_errored.append(file)
        except Exception as e:
            print(f"An error occured while trying to read the following file: {file}\nError message: {e}")

    # Returning the final lists

    return final_complete_list, final_errored, dmain_list

def imaginary_phonon_filter(dmaout_list):

    '''
    This function filters through a list of dmaout files and organises them into two lists:
    1. real_phonon_list - contains all of the dmaout files that do not have any imaginary phonon modes
    2. imaginary_phonon_list - contains all of the dmaout files that have imaginary phonon modes

    INPUT: dmaout_list - list of all the dmaout files to be filtered
    OUTPUT: real_phonon_list - list of all the dmaout files without imaginary phonon modes
            imaginary_phonon_list - list of all the dmaout files with imaginary phonon modes
    '''

    # Creating lists to store files with imaginary and real phonon modes
    imaginary_phonon_list = []
    real_phonon_list = []

    for dmaout_file in dmaout_list:

        # Opening the dmaout file and reading its lines
        with open(dmaout_file, 'r') as file:
            lines = file.readlines()

        # Finding the start index of the phonon frequency section
        start_index = -1
        for i, line in enumerate(lines):
            if "Zone Centre Phonon Frequencies" in line:
                start_index = i + 3
                break
        
        # If the section is not found, consider it as having real phonon modes
        if start_index == -1:
            real_phonon_list.append(dmaout_file)
            continue

        phonon_frequencies = []

        # Extracting phonon frequencies from the relevant section
        for line in lines[start_index:]:
            if not line.strip():
                break
            parts = line.split()
            if len(parts) >= 2:
                try:
                    freq = float(parts[1])
                    phonon_frequencies.append(freq)
                except ValueError:
                    continue
        
        # Checking for imaginary phonon modes
        has_imaginary = any(freq < -0.0001 for freq in phonon_frequencies)

        # Adding the file to the appropriate list based on the presence of imaginary modes
        if has_imaginary:
            imaginary_phonon_list.append(dmaout_file)
        else:
            real_phonon_list.append(dmaout_file)

    # Returning the final lists
    return real_phonon_list, imaginary_phonon_list

def autofree_preparations(real_phonon_list, imaginary_phonon_list, dmain_list, troubleshoot_dmaout_list=None, run_number=run_number, results_directory=results_directory):

    '''
    This function moves dmaout and dmain files to their appropriate directories based on their status:
    1. real_phonon_list - dmaout files without imaginary phonon modes
    2. imaginary_phonon_list - dmaout files with imaginary phonon modes
    3. troubleshoot_dmaout_list - (optional) dmaout files that need troubleshooting

    INPUT: real_phonon_list - list of dmaout files without imaginary phonon modes
           imaginary_phonon_list - list of dmaout files with imaginary phonon modes
           dmain_list - list of all the dmain files
           troubleshoot_dmaout_list - (optional) list of dmaout files that need troubleshooting
           run_number - the run number for directory naming
           results_directory - the base directory to store the results
    OUTPUT: A summary dictionary containing details of moved files and any missing dmain files
    '''

    real_dest_base = f"{results_directory}/crystals-run-{run_number}"
    imaginary_dest_base = f"{results_directory}/crystals-run-{run_number}-with-errors/imaginary-error"
    troubleshoot_dest_base = f"{results_directory}/crystals-run-{run_number}-with-errors/troubleshooting"

    os.makedirs(real_dest_base, exist_ok=True)
    os.makedirs(imaginary_dest_base, exist_ok=True)
    os.makedirs(troubleshoot_dest_base, exist_ok=True)

    directories_to_clean = set()

    def process_file(file_list, dest_base, file_type):
        
        # Creating a list to store moved files and missing dmain files
        moved_files = []
        missing_dmain_files = []

        for file in file_list:
            # Determining the destination directory
            directories_to_clean.add(os.path.dirname(file))
            
            # Constructing the corresponding dmain file path
            dmain_path = file.replace(".dmaout", ".dmain")

            # Checking if the corresponding dmain file exists
            if dmain_path not in dmain_list:
                missing_dmain_files.append(file)
                continue

            # Obtaining the crystal name, subfolder, and filename from the path
            path_parts = file.split(os.sep)
            crystal_name = path_parts[-4]
            subfolder = path_parts[-2]
            filename = path_parts[-1]

            # Constructing the full destination path
            dest_path = os.path.join(dest_base, crystal_name, subfolder, filename)

            # Creating the destination directory if it doesn't exist
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)

            # Moving the dmaout and dmain files to the destination
            try:
                shutil.move(file, dest_path)
                dmain_dest = dest_path.replace(".dmaout", ".dmain")
                shutil.move(dmain_path, dmain_dest)
                moved_files.append((file, dest_path))
            except Exception as e:
                print(f"An error occurred while moving the file {file} to {dest_path}: {e}")
            
        # Returning the list of moved files and missing dmain files
        return moved_files, missing_dmain_files

    # Processing real phonon files
    print("\nProcessing real phonon files...")
    real_moved, real_missing = process_file(real_phonon_list, real_dest_base, "real")

    # Processing imaginary phonon files
    print("\nProcessing imaginary phonon files...")
    imaginary_moved, imaginary_missing = process_file(imaginary_phonon_list, imaginary_dest_base, "imaginary")

    # Processing troubleshooting files if provided
    troubleshoot_moved, troubleshoot_missing = [], []
    if troubleshoot_dmaout_list:
        print("\nProcessing troubleshooting files...")
        troubleshoot_moved, troubleshoot_missing = process_file(troubleshoot_dmaout_list, troubleshoot_dest_base, "troubleshooting")
    
    # Cleaning up empty directories
    print("\nCleaning up empty directories...")
    for directory in directories_to_clean:
        try:
            if not os.listdir(directory):
                os.rmdir(directory)
                print(f"Removed empty directory: {directory}")
        except OSError as e:
            print(f"Error removing directory {directory}: {e}")
    
    # Printing a summary of the operations
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

def run_filtering_pipeline(base_directory=base_directory, run_number=run_number, results_directory=results_directory):

    '''
    This function runs the entire filtering pipeline:
    1. Initial filtering of raw crystal structure data
    2. Filtering based on imaginary phonon modes
    3. Preparing files for AutoFree by moving them to appropriate directories

    INPUT: base_directory - the directory that contains all of the raw crystal structure data
           run_number - the run number for directory naming
           results_directory - the base directory to store the results
    OUTPUT: A summary dictionary containing details of moved files and any missing dmain files
    '''

    # Running the initial filtering stage
    completed_dmaout_list, troubleshoot_dmaout_list, dmain_list = initial_filter(base_directory)

    # Filtering based on imaginary phonon modes
    real_phonon_list, imaginary_phonon_list = imaginary_phonon_filter(completed_dmaout_list)

    # Preparing files for AutoFree by moving them to appropriate directories
    result = autofree_preparations(real_phonon_list, imaginary_phonon_list, dmain_list, troubleshoot_dmaout_list, run_number, results_directory)

    return result

def get_errored_files_list(errored_files_directory):

    '''
    This function retrieves a list of crystal names that have errored data from a specified directory.
    INPUT: errored_files_directory - the directory containing errored crystal structure data
    OUTPUT: errored_crystal_names - list of crystal names with errored data
    '''

    errored_crystal_names = []

    for structure in os.listdir(errored_files_directory):
        structure_path = os.path.join(errored_files_directory, structure)

        for crystal_structure in os.listdir(structure_path):
            crystal_path = os.path.join(structure_path, crystal_structure)
            crystal_name = os.path.basename(crystal_path)
            errored_crystal_names.append(crystal_name)
    
    return errored_crystal_names

def getting_errored_files_real_data(real_files_directory, errored_crystal_names):

    '''
    This function retrieves the directories of crystal structures that have errored data based on a list of errored crystal names.
    INPUT: real_files_directory - the directory containing all of the raw crystal structure data
           errored_crystal_names - list of crystal names with errored data
    OUTPUT: error_files_real_data_directories - list of directories of crystal structures with errored data
    '''

    error_files_real_data_directories = []

    for structure in os.listdir(real_files_directory):
        structure_path = os.path.join(real_files_directory, structure)

        for crystal_structure in os.listdir(structure_path):
            crystal_path = os.path.join(structure_path, crystal_structure)
            crystal_name = os.path.basename(crystal_path)
            print(f'Processing: {crystal_name}')

            if crystal_name in errored_crystal_names:
                print(f'{crystal_name} has errored data')
                error_files_real_data_directories.append(crystal_path)

            else:
                print(f'{crystal_name} does not have errored data')
    
    return error_files_real_data_directories

def moving_errored_files_real_data(error_files_real_data_directory,error_files_real_data_directories):

    '''
    This function moves directories of crystal structures with errored data to a specified directory.
    INPUT: error_files_real_data_directory - the directory to move errored crystal structure data to
           error_files_real_data_directories - list of directories of crystal structures with errored data
    OUTPUT: None
    '''

    os.makedirs(error_files_real_data_directory, exist_ok=True)

    for directory in error_files_real_data_directories:
        directory_name = os.path.basename(directory)
        crystal_name = directory_name[:6]
        
        new_location = os.path.join(error_files_real_data_directory, crystal_name, directory_name)
        print(crystal_name)
        print(directory_name)
        print(new_location)
        print(directory)

        shutil.move(directory, new_location)
        print(f'{directory_name} has been successfully moved')

def run_post_filtering_pipeline(errored_files_directory, real_files_directory, error_files_real_data_directory):

    '''
    This function runs the entire post-filtering pipeline:
    1. Getting a list of crystal names with errored data
    2. Getting the directories of crystal structures with errored data
    3. Moving the directories of crystal structures with errored data to a specified directory

    INPUT: errored_files_directory - the directory containing errored crystal structure data
           real_files_directory - the directory containing all of the raw crystal structure data
           error_files_real_data_directory - the directory to move errored crystal structure data to
    OUTPUT: None
    '''

    # Getting a list of crystal names with errored data
    errored_crystal_names = get_errored_files_list(errored_files_directory)

    # Getting the directories of crystal structures with errored data
    error_files_real_data_directories = getting_errored_files_real_data(real_files_directory, errored_crystal_names)

    # Moving the directories of crystal structures with errored data to a specified directory
    moving_errored_files_real_data(error_files_real_data_directory,error_files_real_data_directories)

def analyse_folder_structure(base_directory):

    '''
    This function analyses the folder structure of a given directory, counting the number of files in each folder.
    INPUT: base_directory - the directory to analyze
    OUTPUT: folder_stats - dictionary with folder paths as keys and file counts as values
            count_distribution - dictionary with file counts as keys and number of folders with that count as values
            folders_by_count - dictionary with file counts as keys and lists of folder paths with that count as values
    '''

    # Creating dictionaries to store the analysis results
    folder_stats = {}
    count_distribution = defaultdict(int)
    folders_by_count = defaultdict(list)

    for root, dirs, files in os.walk(base_directory):
        file_count = len(files) # Counting the number of files in the current folder
        folder_stats[root] = file_count # Storing the file count for the current folder
        count_distribution[file_count] += 1 # Incrementing the count of folders with this file count
        folders_by_count[file_count].append(root) # Adding the folder path to the list for this file count
    
    return folder_stats, count_distribution, folders_by_count

def move_two_file_folders(folders_by_count, target_base):
    
    '''
    This function moves folders with exactly two files to a specified target directory, preserving the last two directory levels.
    INPUT: folders_by_count - dictionary with file counts as keys and lists of folder paths with that count as values
           target_base - the base directory to move the folders to
    OUTPUT: None
    '''

    two_file_folders = folders_by_count.get(2, []) # Getting the list of folders with exactly 2 files
    if not two_file_folders:
        print("\nNo folders with exactly 2 files found.")
        return

    target_dir = os.path.join(target_base, "unrun-dmacrys") # Constructing the target directory path
    os.makedirs(target_dir, exist_ok=True) # Creating the target directory if it doesn't exist

    print(f"\nMoving {len(two_file_folders)} folders with 2 files to {target_dir}")

    moved_count = 0

    for src_path in two_file_folders:
        try:
            # Preserve the last two directory levels (molecule/crystal)
            rel_path = os.path.join(*src_path.split(os.sep)[-2:])
            dest_path = os.path.join(target_dir, rel_path)

            # Create parent directories if needed
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)

            shutil.move(src_path, dest_path) # Moving the folder to the target directory
            moved_count += 1
            print(f"Moved: {rel_path}")

        except Exception as e:
            print(f"Failed to move {src_path}: {str(e)}")

    print(f"\nSuccessfully moved {moved_count} folders")

    if moved_count < len(two_file_folders):
        print(f"Failed to move {len(two_file_folders) - moved_count} folders")

def print_summary(folder_stats, count_distribution):

    '''
    This function prints a summary of the folder structure analysis, including file count distribution and statistics.
    INPUT: folder_stats - dictionary with folder paths as keys and file counts as values
           count_distribution - dictionary with file counts as keys and number of folders with that count as values
    OUTPUT: None
    '''

    print("\n=== File Count Distribution ===")
    print("Number of files | Number of folders")
    print("----------------|------------------")

    for count in sorted(count_distribution.keys()):
        print(f"{count:>15} | {count_distribution[count]:>15}")
    
    total_folders = sum(count_distribution.values())
    avg_files = sum(k*v for k,v in count_distribution.items()) / total_folders
    
    print("\n=== Summary Statistics ===")
    print(f"Total folders analyzed: {total_folders}")
    print(f"Average files per folder: {avg_files:.1f}")
    print(f"Most files in a single folder: {max(count_distribution.keys())}")
    print(f"Least files in a single folder: {min(count_distribution.keys())}")

def unrun_dmacrys_pipeline(base_directory, target_base):

    '''
    This function runs the entire unrun-dmacrys pipeline:
    1. Analyzing the folder structure of a given directory
    2. Saving lists of folder paths grouped by their file counts to text files
    3. Moving folders with exactly two files to a specified target directory
    4. Printing a summary of the folder structure analysis

    INPUT: base_directory - the directory to analyze
           target_base - the base directory to move folders with exactly two files to
    OUTPUT: None
    '''

    # Analyzing the folder structure
    folder_stats, count_distribution, folders_by_count = analyse_folder_structure(base_directory)

    # Moving folders with exactly two files to the specified target directory
    move_two_file_folders(folders_by_count, target_base)

    # Printing a summary of the folder structure analysis
    print_summary(folder_stats, count_distribution)

def adding_autofree_python_file(completed_dirs, autofree_dir):
    '''
    This function copies the AutoFree.py script into each crystal directory within the completed directories.
    '''
    
    # Validate input directory first
    allowed_prefixes = ['/lyceum/la3g22', '/scratch/la3g22']
    
    if isinstance(completed_dirs, str):
        completed_dirs = [completed_dirs]
    
    for dir_path in completed_dirs:
        if not any(dir_path.startswith(prefix) for prefix in allowed_prefixes):
            print(f"CRITICAL ERROR: Attempting to process unauthorized directory: {dir_path}")
            print(f"Allowed prefixes: {allowed_prefixes}")
            return

    # Determine the actual AutoFree.py file path
    if os.path.isfile(autofree_dir):
        autofree_file = autofree_dir
    elif os.path.isdir(autofree_dir):
        autofree_file = os.path.join(autofree_dir, "AutoFree.py")
        if not os.path.isfile(autofree_file):
            print(f"ERROR: AutoFree.py not found in {autofree_dir}")
            return
    else:
        print(f"ERROR: AutoFree path not found: {autofree_dir}")
        return

    print(f"Debug: Using AutoFree.py from: {autofree_file}")

    for directory in completed_dirs:
        print(f"Debug: Processing directory: {directory}")
        
        if not os.path.isdir(directory):
            print(f"Warning: Directory {directory} does not exist. Skipping.")
            continue

        try:
            items = os.listdir(directory)
            print(f"Debug: Found {len(items)} items in {directory}")
            
            for molecule in items:
                molecule_path = os.path.join(directory, molecule)
                
                if not os.path.isdir(molecule_path):
                    continue

                # STRICT filtering - only process expected directories
                if molecule in ['lost+found', '.', '..'] or molecule.startswith('.') or molecule.startswith('tmp'):
                    continue
                    
                # REMOVED the restrictive digit check - accept all valid directory names
                print(f"DEBUG: Processing molecule directory: {molecule}")

                try:
                    sub_items = os.listdir(molecule_path)
                    for crystal in sub_items:
                        crystal_path = os.path.join(molecule_path, crystal)
                        
                        if not os.path.isdir(crystal_path):
                            continue
                            
                        # More strict filtering
                        if crystal in ['lost+found', '.', '..'] or crystal.startswith('.') or crystal.startswith('tmp'):
                            continue
                            
                        # REMOVED the restrictive check here too - just look for crystal patterns
                        # Process if it looks like a crystal directory (contains QR or is in expected format)
                        if '-QR-' in crystal:  # This is the main identifier for crystal directories
                            try:
                                shutil.copy(autofree_file, crystal_path)
                                print(f'Successfully copied AutoFree.py into {crystal_path}')
                            except Exception as e:
                                print(f"Error copying to {crystal_path}: {e}")
                        else:
                            print(f"DEBUG: Skipping non-crystal (no QR pattern): {crystal}")
                        
                except PermissionError:
                    print(f"Warning: Permission denied accessing {molecule_path}. Skipping.")
                    continue
                except OSError as e:
                    print(f"Warning: OS error accessing {molecule_path}: {e}. Skipping.")
                    continue
        except PermissionError:
            print(f"Warning: Permission denied accessing {directory}. Skipping.")
            continue
        except OSError as e:
            print(f"Warning: OS error accessing {directory}: {e}. Skipping.")
            continue

def count_autofree_runs(completed_dirs):
    '''
    This function counts the total number of crystal directories within the completed directories.
    '''
    
    total_runs = 0

    if isinstance(completed_dirs, str):
        completed_dirs = [completed_dirs]
    
    for directory in completed_dirs:
        print(f"DEBUG: Processing directory: {directory}")
        
        if not os.path.isdir(directory):
            print(f"Warning: Directory {directory} does not exist. Skipping.")
            continue

        # CRITICAL: Verify this is actually your intended directory
        expected_base = '/scratch/la3g22'
        if not directory.startswith(expected_base) and not directory.startswith('/lyceum/la3g22'):
            print(f"ERROR: Unexpected directory being scanned: {directory}")
            print(f"This doesn't match expected base paths. Skipping.")
            continue

        try:
            items = os.listdir(directory)
            for molecule in items:
                molecule_path = os.path.join(directory, molecule)
                
                if not os.path.isdir(molecule_path):
                    continue
                
                # STRICT filtering - only process expected directories
                if molecule in ['lost+found', '.', '..'] or molecule.startswith('.') or molecule.startswith('tmp'):
                    continue
                    
                # REMOVED the restrictive digit check
                print(f"DEBUG: Processing molecule directory: {molecule}")

                try:
                    sub_items = os.listdir(molecule_path)
                    for crystal in sub_items:
                        crystal_path = os.path.join(molecule_path, crystal)
                        
                        if not os.path.isdir(crystal_path):
                            continue
                            
                        # More strict filtering
                        if crystal in ['lost+found', '.', '..'] or crystal.startswith('.') or crystal.startswith('tmp'):
                            continue
                            
                        # Only count if it looks like a crystal directory (contains QR pattern)
                        if '-QR-' in crystal:  # This is the reliable identifier
                            total_runs += 1
                            print(f"DEBUG: Found valid crystal directory: {crystal}")
                        else:
                            print(f"DEBUG: Skipping non-crystal (no QR pattern): {crystal}")
                        
                except PermissionError:
                    print(f"Warning: Permission denied accessing {molecule_path}. Skipping.")
                    continue
                except OSError as e:
                    print(f"Warning: OS error accessing {molecule_path}: {e}. Skipping.")
                    continue
                    
        except PermissionError:
            print(f"Warning: Permission denied accessing {directory}. Skipping.")
            continue
        except OSError as e:
            print(f"Warning: OS error accessing {directory}: {e}. Skipping.")
            continue
    
    print(f"DEBUG: Total runs counted: {total_runs}")
    return total_runs

def running_autofree(completed_dirs, total_runs):
    '''
    This function runs the AutoFree.py script in each crystal directory within the completed directories.
    '''
    
    current_run = 0
    
    # Add the same strict filtering used in count_autofree_runs
    allowed_prefixes = ['/scratch/la3g22', '/lyceum/la3g22']
    
    # Ensure completed_dirs is a list
    if isinstance(completed_dirs, str):
        completed_dirs = [completed_dirs]
    
    for directory in completed_dirs:
        # Validate directory path first
        if not any(directory.startswith(prefix) for prefix in allowed_prefixes):
            print(f"ERROR: Skipping unauthorized directory: {directory}")
            continue
            
        if not os.path.isdir(directory):
            print(f"Warning: Directory {directory} does not exist. Skipping.")
            continue

        try:
            items = os.listdir(directory)
            for molecule in items:
                molecule_path = os.path.join(directory, molecule)
                
                if not os.path.isdir(molecule_path):
                    continue
                
                # STRICT filtering - only process expected directories
                if molecule in ['lost+found', '.', '..'] or molecule.startswith('.') or molecule.startswith('tmp'):
                    continue
                    
                print(f"DEBUG: Processing molecule directory: {molecule}")

                try:
                    sub_items = os.listdir(molecule_path)
                    for crystal in sub_items:
                        crystal_path = os.path.join(molecule_path, crystal)
                        
                        if not os.path.isdir(crystal_path):
                            continue
                            
                        # More strict filtering
                        if crystal in ['lost+found', '.', '..'] or crystal.startswith('.') or crystal.startswith('tmp'):
                            continue
                            
                        # Only process if it looks like a crystal directory
                        if '-QR-' in crystal:  # This is the main identifier for crystal directories
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
                        else:
                            print(f"DEBUG: Skipping non-crystal: {crystal}")
                            
                except PermissionError:
                    print(f"Warning: Permission denied accessing {molecule_path}. Skipping.")
                    continue
                except OSError as e:
                    print(f"Warning: OS error accessing {molecule_path}: {e}. Skipping.")
                    continue
                    
        except PermissionError:
            print(f"Warning: Permission denied accessing {directory}. Skipping.")
            continue
        except OSError as e:
            print(f"Warning: OS error accessing {directory}: {e}. Skipping.")
            continue

def deleting_autofree_python_file(completed_dirs):
    '''
    This function deletes the AutoFree.py script from each crystal directory within the completed directories.
    '''
    
    allowed_prefixes = ['/scratch/la3g22', '/lyceum/la3g22']
    
    # Ensure completed_dirs is a list
    if isinstance(completed_dirs, str):
        completed_dirs = [completed_dirs]
    
    deleted_count = 0
    
    for directory in completed_dirs:
        # Validate directory path
        if not any(directory.startswith(prefix) for prefix in allowed_prefixes):
            print(f"ERROR: Skipping unauthorized directory: {directory}")
            continue
            
        if not os.path.isdir(directory):
            print(f"Warning: Directory {directory} does not exist. Skipping.")
            continue

        try:
            items = os.listdir(directory)
            for molecule in items:
                molecule_path = os.path.join(directory, molecule)
                
                if not os.path.isdir(molecule_path):
                    continue
                
                # STRICT filtering
                if molecule in ['lost+found', '.', '..'] or molecule.startswith('.') or molecule.startswith('tmp'):
                    continue

                try:
                    sub_items = os.listdir(molecule_path)
                    for crystal in sub_items:
                        crystal_path = os.path.join(molecule_path, crystal)
                        
                        if not os.path.isdir(crystal_path):
                            continue
                            
                        if crystal in ['lost+found', '.', '..'] or crystal.startswith('.') or crystal.startswith('tmp'):
                            continue
                            
                        if '-QR-' in crystal:
                            try:
                                autofree_path = os.path.join(crystal_path, 'AutoFree.py')
                                if os.path.exists(autofree_path):
                                    os.remove(autofree_path)
                                    deleted_count += 1
                                    print(f'Successfully deleted AutoFree.py from {crystal_path}')
                                else:
                                    print(f"AutoFree.py not found in {crystal_path}")
                            except Exception as e:
                                print(f"Error deleting AutoFree.py from {crystal_path}: {e}")
                        
                except (PermissionError, OSError) as e:
                    print(f"Warning: Error accessing {molecule_path}: {e}. Skipping.")
                    continue
                    
        except (PermissionError, OSError) as e:
            print(f"Warning: Error accessing {directory}: {e}. Skipping.")
            continue
    
    print(f"Total AutoFree.py files deleted: {deleted_count}")

def copy_output_files(completed_dirs, output_base_dir):
    '''
    This function copies .out and .dos files from each crystal directory within the completed directories to a new directory structure.
    '''
    
    # Add the same strict filtering used in other functions
    allowed_prefixes = ['/scratch/la3g22', '/lyceum/la3g22']
    
    # Ensure completed_dirs is a list
    if isinstance(completed_dirs, str):
        completed_dirs = [completed_dirs]
    
    for directory in completed_dirs:
        # Validate directory path first
        if not any(directory.startswith(prefix) for prefix in allowed_prefixes):
            print(f"ERROR: Skipping unauthorized directory: {directory}")
            continue
            
        if not os.path.isdir(directory):
            print(f"Warning: Directory {directory} does not exist. Skipping.")
            continue

        try:
            items = os.listdir(directory)
            for molecule in items:
                molecule_path = os.path.join(directory, molecule)
                
                if not os.path.isdir(molecule_path):
                    continue
                
                # STRICT filtering - only process expected directories
                if molecule in ['lost+found', '.', '..'] or molecule.startswith('.') or molecule.startswith('tmp'):
                    continue
                    
                print(f"DEBUG: Processing molecule directory: {molecule}")

                try:
                    sub_items = os.listdir(molecule_path)
                    for crystal in sub_items:
                        crystal_path = os.path.join(molecule_path, crystal)
                        
                        if not os.path.isdir(crystal_path):
                            continue
                            
                        # More strict filtering
                        if crystal in ['lost+found', '.', '..'] or crystal.startswith('.') or crystal.startswith('tmp'):
                            continue
                            
                        # Only process if it looks like a crystal directory
                        if '-QR-' in crystal:
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
                                else:
                                    print(f"Warning: .out file not found: {out_file}")
                                
                                # Copy .dos files (assuming multiple .dos files may exist)
                                dos_files_copied = 0
                                for file in os.listdir(crystal_path):
                                    if file.endswith('.dos'):
                                        dos_file = os.path.join(crystal_path, file)
                                        shutil.copy2(dos_file, target_crystal_dir)
                                        print(f'Copied {dos_file} to {target_crystal_dir}')
                                        dos_files_copied += 1
                                
                                if dos_files_copied == 0:
                                    print(f"No .dos files found in {crystal_path}")
                                    
                            except Exception as e:
                                print(f"Error copying files from {crystal_path}: {e}")
                        else:
                            print(f"DEBUG: Skipping non-crystal: {crystal}")
                        
                except PermissionError:
                    print(f"Warning: Permission denied accessing {molecule_path}. Skipping.")
                    continue
                except OSError as e:
                    print(f"Warning: OS error accessing {molecule_path}: {e}. Skipping.")
                    continue
        except PermissionError:
            print(f"Warning: Permission denied accessing {directory}. Skipping.")
            continue
        except OSError as e:
            print(f"Warning: OS error accessing {directory}: {e}. Skipping.")
            continue

def run_autofree_pipeline(autofree_completed_directories, autofree_directory, autofree_output_directory):

    '''
    This function runs the entire AutoFree pipeline:
    1. Counting the total number of crystal directories to process
    2. Copying the AutoFree.py script into each crystal directory
    3. Running the AutoFree.py script in each crystal directory and capturing output and errors
    4. Deleting the AutoFree.py script from each crystal directory
    5. Copying .out and .dos files to a new organized directory structure

    INPUT: completed_dirs - list of directories containing completed crystal structure data
           autofree_dir - path to the AutoFree.py script
           autofree_output_directory - the base directory to copy the output files to
    OUTPUT: None
    '''

    print('\nSTAGE 1: Counting AutoFree runs...')
    
    # Ensure autofree_completed_directories is a list
    if isinstance(autofree_completed_directories, str):
        directories_to_process = [autofree_completed_directories]
    else:
        directories_to_process = autofree_completed_directories
    
    total_runs = count_autofree_runs(directories_to_process)
    print(f'Found {total_runs} directories to process')
    print('\n ****************************************** STAGE 1 COMPLETED ******************************************')
    
    print('\nSTAGE 2: Copying AutoFree Files')
    adding_autofree_python_file(directories_to_process, autofree_directory)
    print('\n ****************************************** STAGE 2 COMPLETED ******************************************')
    
    print('\nSTAGE 3: Running AutoFree.py')
    running_autofree(directories_to_process, total_runs)
    print('\n ****************************************** STAGE 3 COMPLETED ******************************************')
    
    print('\nSTAGE 4: Deleting AutoFree Files')
    deleting_autofree_python_file(directories_to_process)
    print('\n ****************************************** STAGE 4 COMPLETED ******************************************')

    print('\nSTAGE 5: Copying output files to organized directory')
    copy_output_files(directories_to_process, autofree_output_directory)
    print('\n ****************************************** STAGE 5 COMPLETED ******************************************')

def generating_res_files_pipeline(working_crystals_directory, autofree_completed_directories, db_source_directory):

    '''
    This function generates .res files for each crystal by:
    1. Collecting all unique crystal names from the completed directories
    2. Searching for matching database files in the source directory
    3. Copying the database files to the appropriate crystal directories
    4. Running the cspy-db dump command to generate .res files
    5. Unzipping the structures.zip file into a structure-files directory
    '''

    # Create the target directory if it doesn't exist
    os.makedirs(working_crystals_directory, exist_ok=True)

    # Ensure autofree_completed_directories is a list
    if isinstance(autofree_completed_directories, str):
        source_dirs = [autofree_completed_directories]
    else:
        source_dirs = autofree_completed_directories

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
    
    # Debug: print the crystal names found
    print(f"Crystal names found: {sorted(crystal_names)}")

    # Now search for matching database files
    db_patterns = [
        "{name}_final.db",
        "{name}_sohncke.db",
        "{name}_cpk.db",
        "{name}.db",
        "{name}_pack.db"
    ]

    processed_count = 0

    for crystal_name in crystal_names:
        found = False
        target_dir = os.path.join(working_crystals_directory, crystal_name)
        
        # Try each possible pattern
        for pattern in db_patterns:
            db_filename = pattern.format(name=crystal_name)
            source_db_path = os.path.join(db_source_directory, db_filename)
            
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
            print(f"Warning: No matching DB file found for {crystal_name} in {db_source_directory}")

    print(f"\nOperation completed. Processed {processed_count} database files out of {len(crystal_names)} crystals.")

def filtering_res_files_pipeline(autofree_completed_directories, working_crystals_directory):
    print("DEBUG: Starting filtering_res_files_pipeline")
    print(f"DEBUG: autofree_completed_directories = {autofree_completed_directories}")
    print(f"DEBUG: working_crystals_directory = {working_crystals_directory}")
    
    # Ensure autofree_completed_directories is a list
    if isinstance(autofree_completed_directories, str):
        autofree_completed_directories = [autofree_completed_directories]
    
    # Get the base directory path (crystals-run-1)
    base_dir = autofree_completed_directories[0]
    
    # Construct the correct paths - they should be in crystals-run-1-with-errors
    base_errors_dir = base_dir + "-with-errors"
    error_directory = os.path.join(base_errors_dir, "imaginary-error")
    unrun_directory = os.path.join(base_errors_dir, "unrun-dmacrys")

    imaginary_directory = os.path.join(working_crystals_directory, "imaginary-crystals")
    unrun_directory_target = os.path.join(working_crystals_directory, "un-run-crystals")

    print(f"DEBUG: base_errors_dir = {base_errors_dir}")
    print(f"DEBUG: error_directory = {error_directory}")
    print(f"DEBUG: unrun_directory = {unrun_directory}")
    print(f"DEBUG: imaginary_directory = {imaginary_directory}")
    print(f"DEBUG: unrun_directory_target = {unrun_directory_target}")

    # Check if directories exist
    print(f"DEBUG: Does base_errors_dir exist? {os.path.exists(base_errors_dir)}")
    print(f"DEBUG: Does error_directory exist? {os.path.exists(error_directory)}")
    print(f"DEBUG: Does unrun_directory exist? {os.path.exists(unrun_directory)}")
    print(f"DEBUG: Does working_crystals_directory exist? {os.path.exists(working_crystals_directory)}")

    # Making sure target directories exist
    os.makedirs(imaginary_directory, exist_ok=True)
    os.makedirs(unrun_directory_target, exist_ok=True)

    def find_crystal_qr_codes(directories):
        '''
        This function finds all crystal QR codes in the given directories.
        '''
        print(f"DEBUG: Finding crystal QR codes in directories: {directories}")
        crystal_qrs = {}

        for directory in directories:
            if not os.path.exists(directory):
                print(f"DEBUG: Directory not found, skipping: {directory}")
                continue
            
            print(f"DEBUG: Walking directory: {directory}")
            try:
                for root, dirs, files in os.walk(directory):
                    print(f"DEBUG: In root: {root}, found {len(dirs)} subdirectories")
                    for dir_name in dirs:
                        if "-QR-" in dir_name:
                            base_name = dir_name.split("-QR-")[0]
                            crystal_qrs[dir_name] = {
                                "path": os.path.join(root, dir_name),
                                "base_name": base_name
                            }
                            print(f"DEBUG: Found crystal QR: {dir_name} with base: {base_name}")
            except Exception as e:
                print(f"DEBUG: Error walking directory {directory}: {e}")

        print(f"DEBUG: Total crystal QR codes found: {len(crystal_qrs)}")
        return crystal_qrs
    
    # Find all crystal QR codes in each type of directory
    print("DEBUG: Finding normal crystals...")
    normal_crystals = find_crystal_qr_codes(autofree_completed_directories)
    
    print("DEBUG: Finding imaginary crystals...")
    imaginary_crystals = find_crystal_qr_codes([error_directory])
    
    print("DEBUG: Finding unrun crystals...")
    unrun_crystals = find_crystal_qr_codes([unrun_directory])

    print(f"DEBUG: Found {len(normal_crystals)} normal crystals")
    print(f"DEBUG: Found {len(imaginary_crystals)} imaginary crystals")
    print(f"DEBUG: Found {len(unrun_crystals)} unrun crystals")

    # Process the .res files
    all_crystals = {**normal_crystals, **imaginary_crystals, **unrun_crystals}
    print(f"DEBUG: Processing {len(all_crystals)} total crystals")
    
    processed_count = 0
    for crystal_qr, info in all_crystals.items():
        base_name = info["base_name"]
        res_file = os.path.join(working_crystals_directory, base_name, "structure-files", f"{crystal_qr}.res")
        
        print(f"DEBUG: Looking for .res file: {res_file}")
        
        if not os.path.exists(res_file):
            print(f"DEBUG: .res file not found for {crystal_qr}")
            continue
        
        # Determine where to move/delete based on origin
        if crystal_qr in normal_crystals:
            # Delete .res file for normal crystals
            try:
                os.remove(res_file)
                print(f"Deleted .res file for normal crystal: {crystal_qr}")
                processed_count += 1
            except Exception as e:
                print(f"Error deleting .res file for {crystal_qr}: {e}")

        elif crystal_qr in imaginary_crystals:
            # Move to imaginary-crystals
            dest_dir = os.path.join(imaginary_directory, base_name)
            os.makedirs(dest_dir, exist_ok=True)
            try:
                shutil.move(res_file, os.path.join(dest_dir, f"{crystal_qr}.res"))
                print(f"Moved to imaginary-crystals: {crystal_qr}")
                processed_count += 1
            except Exception as e:
                print(f"Error moving {crystal_qr} to imaginary-crystals: {e}")

        elif crystal_qr in unrun_crystals:
            # Move to un-run-crystals
            dest_dir = os.path.join(unrun_directory_target, base_name)
            os.makedirs(dest_dir, exist_ok=True)
            try:
                shutil.move(res_file, os.path.join(dest_dir, f"{crystal_qr}.res"))
                print(f"Moved to un-run-crystals: {crystal_qr}")
                processed_count += 1
            except Exception as e:
                print(f"Error moving {crystal_qr} to un-run-crystals: {e}")

    print(f"DEBUG: Processed {processed_count} .res files")
    print("Operation completed.")

def process_crystal(crystal_name, out_files_root, initial_info_directory, output_dir):
    
    def process_out_file(out_path):
        '''
        This function extracts the KDE vibrational energy from a given .out file.
        INPUT: out_path - path to the .out file
        OUTPUT: kde_energy - extracted KDE vibrational energy as a float, or none if there is none
        '''

        # Only look for the KDE vibrational energy in .out files
        kde_pattern = r"Epanechnikov KDE vibrational energy:\s+([-\d.]+)\s+kJ/mol"

        # Looking for the pattern in the .out file
        try:
            with open(out_path, 'r') as f:
                content = f.read()
                match = re.search(kde_pattern, content)
                if match:
                    return float(match.group(1))
            return None
        
        except Exception as e:
            print(f"Error reading {out_path}: {e}")
            return None
    
    '''
    This function processes all files for one crystal using only KDE vibrational energy.
    INPUT: crystal_name - name of the crystal to process
    OUTPUT: None
    '''

    # Ensure paths are Path objects
    out_files_root = Path(out_files_root)
    initial_info_directory = Path(initial_info_directory)
    output_dir = Path(output_dir)

    # Paths for this crystal
    crystal_out_dir = out_files_root / crystal_name
    initial_csv = initial_info_directory / f"{crystal_name}-structures.csv"
    output_crystal_dir = output_dir / crystal_name
    successful_csv = output_crystal_dir / "successful-structures.csv"
    unsuccessful_csv = output_crystal_dir / "unsuccessful-structures.csv"

    # Create output directories
    output_crystal_dir.mkdir(parents=True, exist_ok=True)

    if not initial_csv.exists():
        print(f"Warning: Initial CSV not found for {crystal_name}")
        return

    # Read all structure IDs that have .out files
    out_structure_ids = set()
    for root, _, files in os.walk(crystal_out_dir):
        for file in files:
            if file.endswith('.out'):
                structure_id = Path(root).stem
                out_structure_ids.add(structure_id)

    # Process the initial CSV
    successful_rows = []
    unsuccessful_rows = []

    with open(initial_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['id'] in out_structure_ids:
                # Add KDE vibrational energy if available
                out_file = crystal_out_dir / row['id'] / f"{row['id']}.out"
                kde_energy = process_out_file(out_file)
                if kde_energy is not None:
                    row['KDE Vibrational Energy (kJ/mol)'] = kde_energy
                    successful_rows.append(row)
            else:
                unsuccessful_rows.append(row)

    # Process successful rows with rankings
    if successful_rows:
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(successful_rows)
        
        # Convert energy columns to numeric
        df['Initial Energy (kJ/mol)'] = pd.to_numeric(df['energy'], errors='coerce')
        df['KDE Vibrational Energy (kJ/mol)'] = pd.to_numeric(df['KDE Vibrational Energy (kJ/mol)'], errors='coerce')
        
        # Calculate total final energy (initial + KDE)
        df['Total Energy (kJ/mol)'] = df['Initial Energy (kJ/mol)'] + df['KDE Vibrational Energy (kJ/mol)']
        
        # Create rankings
        df['Initial Rank'] = df['Initial Energy (kJ/mol)'].rank(method='min', ascending=True).astype('Int64')
        df['Final Rank'] = df['Total Energy (kJ/mol)'].rank(method='min', ascending=True).astype('Int64')
        df['Rank Change'] = (df['Initial Rank'] - df['Final Rank']).astype('Int64')
        
        # Select and rename columns
        final_columns = {
            'id': 'Crystal ID',
            'spacegroup': 'Spacegroup',
            'density': 'Density',
            'Initial Energy (kJ/mol)': 'Initial Energy (kJ/mol)',
            'KDE Vibrational Energy (kJ/mol)': 'KDE Vibrational Energy (kJ/mol)',
            'Total Energy (kJ/mol)': 'Total Energy (kJ/mol)',
            'Initial Rank': 'Initial Rank',
            'Final Rank': 'Final Rank',
            'Rank Change': 'Rank Change'
        }
        
        # Filter and rename columns
        df = df[list(final_columns.keys())].rename(columns=final_columns)
        
        # Sort by Final Rank (best first)
        df = df.sort_values('Final Rank', ascending=True)
        
        # Write to CSV
        df.to_csv(successful_csv, index=False)
        print(f"Saved {len(df)} successful structures for {crystal_name}")
    
    # Write unsuccessful structures
    if unsuccessful_rows:
        unsuccessful_data = []
        for row in unsuccessful_rows:
            unsuccessful_data.append({
                'Crystal ID': row['id'],
                'Spacegroup': row['spacegroup'],
                'Density': row['density'],
                'Initial Energy (kJ/mol)': row['energy']
            })
        
        unsuccessful_df = pd.DataFrame(unsuccessful_data)
        unsuccessful_df.to_csv(unsuccessful_csv, index=False)
        print(f"Saved {len(unsuccessful_rows)} unsuccessful structures for {crystal_name}")

def out_file_analysis_pipeline(out_files_root=autofree_output_directory, initial_info_dir=initial_info_directory, output_dir=output_directory_for_organised_structure_csvs):
    '''
    This function runs the entire out file analysis pipeline:
    1. Processing each crystal to extract KDE vibrational energies and generate summary CSVs
    2. Saving successful and unsuccessful structure data to separate CSV files
    '''

    # Convert string paths to Path objects
    out_files_root = Path(out_files_root)
    initial_info_dir = Path(initial_info_dir)
    output_dir = Path(output_dir)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process each crystal directory in the out files root
    for crystal_name in os.listdir(out_files_root):
        crystal_path = out_files_root / crystal_name
        if crystal_path.is_dir():
            print(f"\nProcessing crystal: {crystal_name}")
            process_crystal(crystal_name, out_files_root, initial_info_dir, output_dir)
        else:
            print(f"Skipping non-directory item: {crystal_name}")

def generating_report_pipeline(report_directory=report_directory, observed_file=final_ranks_directory, results_dir=output_directory_for_organised_structure_csvs):

    '''
    This function generates a comprehensive report by:
    1. Reading observed crystal structures and their CSP matches
    2. Extracting energy and ranking information from successful structure files
    3. Compiling analysis, raw data, and comparison data into CSV files
    4. Logging warnings and errors encountered during processing

    INPUT: report_directory - directory to save the report files
           observed_file - CSV file containing observed crystal structures and their CSP matches
           results_dir - directory containing successful structure CSV files for each crystal
    OUTPUT: None
    '''
    # New output paths within the report directory
    analysis_output_file = f"{report_directory}/observed_crystals_analysis.csv"
    raw_output_file = f"{report_directory}/observed_crystals_raw_data.csv"
    comparison_output_file = f"{report_directory}/energy_comparison_data.csv"
    log_file = f"{report_directory}/processing_log.txt"

        # Set up logging
    with open(log_file, 'w') as f:
        f.write("Processing Log\n")
        f.write("==============\n\n")

    def log_message(message):
        print(message)
        with open(log_file, 'a') as f:
            f.write(message + "\n")

    # Read observed structures
    observed_df = pd.read_csv(observed_file)

    # Clean the data - drop rows where CSP_Match is NaN
    observed_df = observed_df.dropna(subset=['CSP_Match'])
    observed_df['Crystal_ID'] = observed_df['CSP_Match']

    # Prepare output dataframes
    analysis_columns = [
        'Crystal_ID', 'Original_Rank', 'Final_Rank', 'Total_Vibrational_Energy',
        'Initial_Energy_Diff_vs_Rank1', 'Final_Energy_Diff_vs_Rank1'
    ]
    analysis_data = []

    raw_data_columns = None
    raw_data = []

    # New comparison data structure
    comparison_columns = None
    comparison_data = []

    # Counters for statistics
    total_processed = 0
    found_in_success = 0
    missing_files = 0
    other_errors = 0

    # Process each observed crystal
    for idx, row in observed_df.iterrows():
        try:
            crystal_id = row['Crystal_ID']
            total_processed += 1
            
            # Skip if crystal_id is not a string
            if not isinstance(crystal_id, str):
                log_message(f"Row {idx}: Warning: Skipping non-string Crystal_ID: {crystal_id}")
                other_errors += 1
                continue
                
            # Extract prefix (e.g., 'abudad' from 'abudad-QR-14-9208-3')
            if '-QR-' not in crystal_id:
                log_message(f"Row {idx}: Warning: Skipping malformed Crystal_ID (missing '-QR-'): {crystal_id}")
                other_errors += 1
                continue
                
            prefix = crystal_id.split('-QR-')[0]
            
            # Find the corresponding successful-structures file
            success_file = os.path.join(results_dir, prefix, "successful-structures.csv")
            
            if not os.path.exists(success_file):
                # Try alternative naming (just in case)
                alt_file = os.path.join(results_dir, prefix, f"{prefix}-successful-structures.csv")
                if os.path.exists(alt_file):
                    success_file = alt_file
                else:
                    log_message(f"Row {idx}: Warning: No successful-structures file found for {prefix} (looking for: {success_file})")
                    missing_files += 1
                    continue
            
            # Read the successful structures
            try:
                success_df = pd.read_csv(success_file)
            except Exception as e:
                log_message(f"Row {idx}: Error reading {success_file}: {str(e)}")
                other_errors += 1
                continue
            
            # Find the observed crystal in successful structures
            try:
                crystal_data = success_df[success_df['Crystal ID'] == crystal_id]
                
                if crystal_data.empty:
                    # Try alternative column name
                    if 'Crystal_ID' in success_df.columns:
                        crystal_data = success_df[success_df['Crystal_ID'] == crystal_id]
                    
                    if crystal_data.empty:
                        log_message(f"Row {idx}: Warning: Crystal {crystal_id} not found in successful structures")
                        other_errors += 1
                        continue
                
                crystal_data = crystal_data.iloc[0]
                found_in_success += 1
                
                # Store raw data
                if raw_data_columns is None:
                    raw_data_columns = success_df.columns.tolist()
                raw_data.append(crystal_data.tolist())
                
                # Get initial and final rank 1 crystals
                try:
                    initial_rank1 = success_df[success_df['Initial Rank'] == 1].iloc[0]
                    final_rank1 = success_df[success_df['Final Rank'] == 1].iloc[0]
                    
                    # Prepare the analysis row
                    analysis_row = {
                        'Crystal_ID': crystal_id,
                        'Original_Rank': int(crystal_data['Initial Rank']),
                        'Final_Rank': int(crystal_data['Final Rank']),
                    }
                    
                    # Handle vibrational energy
                    vib_energy_col = None
                    for col in ['Total Vibrational Energy (kJ/mol)', 'KDE Vibrational Energy (kJ/mol)', 'Vibrational Energy']:
                        if col in crystal_data:
                            vib_energy_col = col
                            break
                    
                    if vib_energy_col:
                        analysis_row['Total_Vibrational_Energy'] = round(float(crystal_data[vib_energy_col]), 4)
                    else:
                        analysis_row['Total_Vibrational_Energy'] = None
                    
                    # Calculate INITIAL energy difference
                    initial_energy_col = 'Total Initial Energy (kJ/mol)'
                    if crystal_data['Initial Rank'] == 1:
                        analysis_row['Initial_Energy_Diff_vs_Rank1'] = 0.0
                    else:
                        if initial_energy_col in crystal_data and initial_energy_col in initial_rank1:
                            initial_diff = round(float(crystal_data[initial_energy_col]) - float(initial_rank1[initial_energy_col]))
                            analysis_row['Initial_Energy_Diff_vs_Rank1'] = initial_diff
                        else:
                            analysis_row['Initial_Energy_Diff_vs_Rank1'] = None
                    
                    # Calculate FINAL energy difference
                    final_energy_col = 'Total Final Energy (kJ/mol)'
                    if crystal_data['Final Rank'] == 1:
                        analysis_row['Final_Energy_Diff_vs_Rank1'] = 0.0
                    else:
                        if final_energy_col in crystal_data and final_energy_col in final_rank1:
                            final_diff = round(float(crystal_data[final_energy_col]) - float(final_rank1[final_energy_col]))
                            analysis_row['Final_Energy_Diff_vs_Rank1'] = final_diff
                        else:
                            analysis_row['Final_Energy_Diff_vs_Rank1'] = None
                    
                    analysis_data.append(analysis_row)
                    
                    # Prepare comparison data
                    if comparison_columns is None:
                        comparison_columns = success_df.columns.tolist() + ['Comparison_Type']
                    
                    # Add initial rank1 if different from observed
                    if crystal_data['Initial Rank'] != 1:
                        initial_rank1_row = initial_rank1.tolist() + ['Initial_Rank1']
                        comparison_data.append(initial_rank1_row)
                    
                    # Add observed crystal
                    observed_row = crystal_data.tolist() + ['Observed_Crystal']
                    comparison_data.append(observed_row)
                    
                    # Add final rank1 if different from observed
                    if crystal_data['Final Rank'] != 1:
                        final_rank1_row = final_rank1.tolist() + ['Final_Rank1']
                        comparison_data.append(final_rank1_row)
                    
                except Exception as e:
                    log_message(f"Row {idx}: Error processing energy calculations for {crystal_id}: {str(e)}")
                    other_errors += 1
                    continue
                    
            except Exception as e:
                log_message(f"Row {idx}: Error finding crystal in successful structures: {str(e)}")
                other_errors += 1
                continue
                
        except Exception as e:
            log_message(f"Row {idx}: Error processing row: {str(e)}")
            other_errors += 1
            continue

    # Save analysis data
    if analysis_data:
        analysis_df = pd.DataFrame(analysis_data, columns=analysis_columns)
        analysis_df.to_csv(analysis_output_file, index=False)
        log_message(f"\nAnalysis results saved to {analysis_output_file}")
    else:
        log_message("\nNo analysis data was processed.")

    # Save raw data
    if raw_data and raw_data_columns:
        raw_df = pd.DataFrame(raw_data, columns=raw_data_columns)
        raw_df.to_csv(raw_output_file, index=False)
        log_message(f"Raw data saved to {raw_output_file}")
    else:
        log_message("No raw data was processed.")

    # Save comparison data
    if comparison_data and comparison_columns:
        comparison_df = pd.DataFrame(comparison_data, columns=comparison_columns)
        comparison_df.to_csv(comparison_output_file, index=False)
        log_message(f"Energy comparison data saved to {comparison_output_file}")
    else:
        log_message("No comparison data was processed.")

    # Print summary statistics
    log_message("\nProcessing Summary:")
    log_message(f"Total observed crystals: {len(observed_df)}")
    log_message(f"Total processed: {total_processed}")
    log_message(f"Found in successful-structures.csv: {found_in_success}")
    log_message(f"Missing successful-structures files: {missing_files}")
    log_message(f"Other errors: {other_errors}")

    if found_in_success > 0:
        log_message(f"\nSuccessfully processed {found_in_success} of {len(observed_df)} observed crystals ({found_in_success/len(observed_df)*100:.1f}%)")
    else:
        log_message("\nNo crystals were successfully processed. Check the log for details.")

def find_successful_structures_files(base_dir=output_directory_for_organised_structure_csvs):

    '''
    This function finds all successful-structures.csv files in the given base directory.

    INPUT: base_dir - the base directory to search for successful-structures.csv files
    OUTPUT: csv_files - list of paths to successful-structures.csv files
    '''
    csv_files = []
    for root, dirs, files in os.walk(base_dir):
        if "successful-structures.csv" in files:
            csv_files.append(Path(root) / "successful-structures.csv")
    return csv_files

def read_observed_crystals(path=final_ranks_directory):

    '''
    This function reads the observed crystals data into a dictionary keyed by CSP_Match.

    INPUT: path - path to the observed crystals CSV file
    OUTPUT: observed - dictionary with CSP_Match as keys and row data as values
    '''

    observed = {}
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            csp_match = row['CSP_Match']
            observed[csp_match] = row
    
    return observed

def process_files(csv_files, observed_data):

    '''
    This function processes each successful-structures.csv file to extract initial rank 1, final rank
    1, and observed crystals matching CSP_Match entries.

    INPUT: csv_files - list of paths to successful-structures.csv files
           observed_data - dictionary with CSP_Match as keys and row data as values
    OUTPUT: results - list of dictionaries containing processed data for each directory
    '''

    results = []
    
    # First create a mapping of all crystal IDs to their data
    all_crystals = {}
    for csv_file in csv_files:
        dir_name = csv_file.parent.name
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                crystal_id = row['Crystal ID']
                full_id = f"{dir_name}-{crystal_id}"
                all_crystals[full_id] = row
                all_crystals[crystal_id] = row  # Also store without prefix
    
    # Now process each directory
    for csv_file in csv_files:
        dir_name = csv_file.parent.name
        
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            # Find initial rank 1
            initial_rank1 = next((row for row in rows if row['Initial Rank'] == '1'), None)
            
            # Find final rank 1
            final_rank1 = next((row for row in rows if row['Final Rank'] == '1'), None)
            
            # Find all possible observed crystals for this directory
            observed_rows = []
            for obs_id, obs_data in observed_data.items():
                if obs_id.startswith(dir_name):
                    # Try to find matching crystal data
                    crystal_data = all_crystals.get(obs_id)
                    if crystal_data:
                        observed_rows.append({
                            'csp_match': obs_id,
                            'data': crystal_data
                        })
                    else:
                        # If no exact match, try with just the QR part
                        qr_part = obs_id.split('-', 1)[1]
                        for crystal_id, crystal_row in all_crystals.items():
                            if crystal_id.endswith(qr_part):
                                observed_rows.append({
                                    'csp_match': obs_id,
                                    'data': crystal_row
                                })
                                break
            
            results.append({
                'directory': dir_name,
                'initial_rank1': initial_rank1,
                'final_rank1': final_rank1,
                'observed_crystals': observed_rows
            })
    
    return results

def write_output(results, output_path):

    '''
    This function writes the processed results to a CSV file.

    INPUT: results - list of processed results for each directory
           output_path - path to save the output CSV file
    OUTPUT: None
    '''
    
    fieldnames = [
        'Directory',
        'Type',
        'Crystal ID',
        'Initial Energy (kJ/mol)',
        'Total Energy (kJ/mol)',
        'Initial Rank',
        'Final Rank'
    ]
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for result in results:
            # Write initial rank 1 row
            if result['initial_rank1']:
                writer.writerow({
                    'Directory': result['directory'],
                    'Type': 'Initial Rank1',
                    'Crystal ID': result['initial_rank1']['Crystal ID'],
                    'Initial Energy (kJ/mol)': result['initial_rank1']['Initial Energy (kJ/mol)'],
                    'Total Energy (kJ/mol)': result['initial_rank1']['Total Energy (kJ/mol)'],
                    'Initial Rank': result['initial_rank1']['Initial Rank'],
                    'Final Rank': result['initial_rank1']['Final Rank']
                })
            
            # Write final rank 1 row
            if result['final_rank1']:
                writer.writerow({
                    'Directory': result['directory'],
                    'Type': 'Final Rank1',
                    'Crystal ID': result['final_rank1']['Crystal ID'],
                    'Initial Energy (kJ/mol)': result['final_rank1']['Initial Energy (kJ/mol)'],
                    'Total Energy (kJ/mol)': result['final_rank1']['Total Energy (kJ/mol)'],
                    'Initial Rank': result['final_rank1']['Initial Rank'],
                    'Final Rank': result['final_rank1']['Final Rank']
                })
            
            # Write observed crystal rows
            for obs in result['observed_crystals']:
                writer.writerow({
                    'Directory': result['directory'],
                    'Type': 'Observed',
                    'Crystal ID': obs['csp_match'],
                    'Initial Energy (kJ/mol)': obs['data']['Initial Energy (kJ/mol)'],
                    'Total Energy (kJ/mol)': obs['data']['Total Energy (kJ/mol)'],
                    'Initial Rank': obs['data']['Initial Rank'],
                    'Final Rank': obs['data']['Final Rank']
                })

def fixed_crystal_info_pipeline(base_dir=output_directory_for_organised_structure_csvs, observed_crystals_path=final_ranks_directory, report_directory=report_directory):

    '''
    This function generates a CSV file containing information about:
    1. Initial rank 1 crystal for each directory
    2. Final rank 1 crystal for each directory
    3. Observed crystals that match CSP_Match entries

    INPUT: base_dir - directory containing successful-structures.csv files
           observed_crystals_path - path to the observed crystals CSV file
           output_csv_path - path to save the output CSV file
    OUTPUT: None
    '''

    output_csv_path = f"{report_directory}/complete_crystal_info_fixed.csv"

    csv_files = find_successful_structures_files(base_dir)
    print(f"Found {len(csv_files)} CSV files to process")
    
    observed_data = read_observed_crystals(observed_crystals_path)
    print(f"Loaded {len(observed_data)} observed crystal entries")
    
    results = process_files(csv_files, observed_data)
    
    write_output(results, output_csv_path)
    print(f"Results written to {output_csv_path}")

def energy_difference_report_pipeline(report_directory=report_directory):
    '''
    This function reads the complete_crystal_info_fixed.csv file, calculates energy differences
    between observed crystals and rank 1 crystals, and writes the results to a new CSV file sorted alphabetically.
    '''

    input_csv = f"{report_directory}/complete_crystal_info_fixed.csv"
    output_csv = f"{report_directory}/energy_differences_sorted_3dp.csv"

    # Read the input CSV and organize the data by directory
    data = {}
    with open(input_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            directory = row['Directory']
            if directory not in data:
                data[directory] = {}
            
            if row['Type'] == 'Initial Rank1':
                data[directory]['initial_rank1'] = row
            elif row['Type'] == 'Final Rank1':
                data[directory]['final_rank1'] = row
            elif row['Type'] == 'Observed':
                if 'observed' not in data[directory]:
                    data[directory]['observed'] = []
                data[directory]['observed'].append(row)

    # Prepare the output data and sort alphabetically by directory
    output_data = []
    sorted_directories = sorted(data.keys())  # Sort directories alphabetically

    for directory in sorted_directories:
        entries = data[directory]
        
        # Check if we have both rank1 entries (required)
        if 'initial_rank1' not in entries or 'final_rank1' not in entries:
            print(f"DEBUG: Skipping {directory} - missing rank1 entries")
            continue
            
        initial_rank1 = entries['initial_rank1']
        final_rank1 = entries['final_rank1']
        
        # If there are observed crystals, process them
        if 'observed' in entries:
            # Sort observed crystals alphabetically by their Crystal ID
            sorted_observed = sorted(entries['observed'], key=lambda x: x['Crystal ID'])
            
            for observed in sorted_observed:
                try:
                    # Calculate initial energy difference (observed vs initial rank1)
                    initial_energy_diff = round(
                        float(observed['Initial Energy (kJ/mol)']) - float(initial_rank1['Initial Energy (kJ/mol)']),
                        3
                    )
                    
                    # Calculate total energy difference (observed vs final rank1)
                    total_energy_diff = round(
                        float(observed['Total Energy (kJ/mol)']) - float(final_rank1['Total Energy (kJ/mol)']),
                        3
                    )
                    
                    output_data.append({
                        'Directory': directory,
                        'Observed Crystal ID': observed['Crystal ID'],
                        'Initial Rank1 Crystal ID': initial_rank1['Crystal ID'],
                        'Final Rank1 Crystal ID': final_rank1['Crystal ID'],
                        'Initial Energy Difference (kJ/mol)': initial_energy_diff,
                        'Total Energy Difference (kJ/mol)': total_energy_diff,
                        'Observed Initial Rank': observed['Initial Rank'],
                        'Observed Final Rank': observed['Final Rank']
                    })
                except (ValueError, TypeError) as e:
                    # Skip if energy values are missing or invalid
                    print(f"DEBUG: Error processing observed crystal {observed.get('Crystal ID', 'unknown')} in {directory}: {e}")
                    continue
        else:
            # No observed crystals - include the directory with rank1 info only
            print(f"DEBUG: No observed crystals found for {directory}, including rank1 info only")
            output_data.append({
                'Directory': directory,
                'Observed Crystal ID': 'N/A',
                'Initial Rank1 Crystal ID': initial_rank1['Crystal ID'],
                'Final Rank1 Crystal ID': final_rank1['Crystal ID'],
                'Initial Energy Difference (kJ/mol)': 0.0,  # Rank1 vs itself
                'Total Energy Difference (kJ/mol)': 0.0,    # Rank1 vs itself
                'Observed Initial Rank': 'N/A',
                'Observed Final Rank': 'N/A'
            })

    # Write the output CSV
    fieldnames = [
        'Directory',
        'Observed Crystal ID',
        'Initial Rank1 Crystal ID',
        'Final Rank1 Crystal ID',
        'Initial Energy Difference (kJ/mol)',
        'Total Energy Difference (kJ/mol)',
        'Observed Initial Rank',
        'Observed Final Rank'
    ]

    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_data)

    print(f"Sorted energy differences (3 decimal places) saved to {output_csv}")
    print(f"Total entries: {len(output_data)}")

def cleanup_working_crystal_dbs(working_crystals_directory):

    '''
    This function cleans up the working_crystal_dbs directory by:
    1. Deleting structure-files folders
    2. Deleting structures.csv files
    3. Moving .db files to the main working_crystal_dbs directory
    4. Removing empty crystal folders
    '''
    
    working_dir = Path(working_crystals_directory)
    
    if not working_dir.exists():
        print(f"Error: Working directory {working_dir} does not exist")
        return
    
    print(f"Cleaning up {working_dir}")
    
    # Keep track of what we process
    db_files_moved = 0
    structure_folders_deleted = 0
    csv_files_deleted = 0
    folders_removed = 0
    
    # Process each item in the working directory
    for item in working_dir.iterdir():
        if item.is_dir():
            # Skip the special directories we want to keep
            if item.name in ['imaginary-crystals', 'un-run-crystals']:
                print(f"Skipping special directory: {item.name}")
                continue
                
            print(f"Processing crystal directory: {item.name}")
            
            # Process files in this crystal directory
            for subitem in item.iterdir():
                if subitem.is_dir() and subitem.name == "structure-files":
                    # Delete structure-files folder
                    try:
                        shutil.rmtree(subitem)
                        structure_folders_deleted += 1
                        print(f"  Deleted structure-files folder: {subitem}")
                    except Exception as e:
                        print(f"  Error deleting {subitem}: {e}")
                
                elif subitem.is_file() and subitem.name == "structures.csv":
                    # Delete structures.csv file
                    try:
                        subitem.unlink()
                        csv_files_deleted += 1
                        print(f"  Deleted CSV file: {subitem}")
                    except Exception as e:
                        print(f"  Error deleting {subitem}: {e}")
                
                elif subitem.is_file() and subitem.suffix == ".db":
                    # Move .db file to main directory
                    try:
                        new_location = working_dir / subitem.name
                        shutil.move(str(subitem), str(new_location))
                        db_files_moved += 1
                        print(f"  Moved DB file: {subitem.name} -> {new_location}")
                    except Exception as e:
                        print(f"  Error moving {subitem}: {e}")
            
            # Check if directory is empty and remove it
            try:
                if not any(item.iterdir()):
                    item.rmdir()
                    folders_removed += 1
                    print(f"  Removed empty directory: {item}")
                else:
                    print(f"  Directory not empty, keeping: {item}")
            except Exception as e:
                print(f"  Error checking/removing directory {item}: {e}")
    
    print(f"\nCleanup completed:")
    print(f"  DB files moved: {db_files_moved}")
    print(f"  Structure folders deleted: {structure_folders_deleted}")
    print(f"  CSV files deleted: {csv_files_deleted}")
    print(f"  Folders removed: {folders_removed}")

def processing():
    
    print('Starting processing procedure.')

    print('\n****************************** STAGE 1: Filtering Data ******************************')
    
    results = run_filtering_pipeline()

    print('\n********************************* STAGE 1: Completed ********************************')

    print('\n******************************** STAGE 2: Post-Filter ********************************')

    main_dir = autofree_completed_directories
    real_files_directory = f'{main_dir}'
    errored_files_directory = f'{main_dir}-with-errors/imaginary-error'
    error_files_real_data_directory = f'{main_dir}-with-errors/real-part-of-errored-data'
    run_post_filtering_pipeline(errored_files_directory, real_files_directory, error_files_real_data_directory)

    print('\n********************************* STAGE 2: Completed ********************************')

    print('\n*************************** STAGE 3: Unrun-DMACRYS Filter ***************************')

    main_dir = autofree_completed_directories
    s3_base_directory = f'{main_dir}'
    s3_target_directory = f'{main_dir}-with-errors'
    unrun_dmacrys_pipeline(s3_base_directory, s3_target_directory)

    print('\n********************************* STAGE 3: Completed ********************************')

    print('\n**************************** STAGE 4: Running AutoFree.py ****************************')

    run_autofree_pipeline(autofree_completed_directories, autofree_directory, autofree_output_directory)

    print('\n********************************* STAGE 4: Completed ********************************')

    print('\n*************************** STAGE 5: Generating .res files ***************************')

    generating_res_files_pipeline(working_crystals_directory, [autofree_completed_directories], db_source_directory)

    print('\n********************************* STAGE 5: Completed ********************************')
    
    print('\n*************************** STAGE 6: Filtering .res files ***************************')

    filtering_res_files_pipeline([autofree_completed_directories], working_crystals_directory)
    cleanup_working_crystal_dbs(working_crystals_directory)

    print('\n********************************* STAGE 6: Completed ********************************')

    print('\n************************** STAGE 7: Analysing .out files **************************')

    out_file_analysis_pipeline()

    print('\n********************************* STAGE 7: Completed ********************************')

    print('\n***************************** STAGE 8: Generating report *****************************')

    generating_report_pipeline()

    print('\n********************************* STAGE 8: Completed ********************************')

    print('\n******************* STAGE 9: Generating Fixed Crystal Info Report *******************')

    fixed_crystal_info_pipeline()

    print('\n********************************* STAGE 9: Completed ********************************')

    print('\n******************** STAGE 10: Generating Energy Difference Report ********************')

    energy_difference_report_pipeline()

    print('\n********************************* STAGE 10: Completed ********************************')

    print('\nProcessing procedure has been complete.')

if __name__ == "__main__":

    processing()

