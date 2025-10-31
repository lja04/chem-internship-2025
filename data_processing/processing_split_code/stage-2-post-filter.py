import os
import shutil

real_files_directory = 'testing'
errored_files_directory = 'testing/imaginary-error'
error_files_real_data_directory = 'real-part-of-errored-data'

def getting_errored_files_list(errored_files_directory):

    errored_crystal_names = []

    for structure in os.listdir(errored_files_directory):
        structure_path = os.path.join(errored_files_directory, structure)

        for crystal_structure in os.listdir(structure_path):
            crystal_path = os.path.join(structure_path, crystal_structure)
            crystal_name = os.path.basename(crystal_path)
            errored_crystal_names.append(crystal_name)
    
    return errored_crystal_names

def getting_errored_files_real_data(real_files_directory, errored_crystal_names):

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



errored_crystal_names = getting_errored_files_list(errored_files_directory)

with open('first.txt', 'w') as f:
    for item in errored_crystal_names:
        f.write(f"{item}\n")

error_files_real_data_directories = getting_errored_files_real_data(real_files_directory, errored_crystal_names)

with open('second.txt', 'w') as f:
    for item in error_files_real_data_directories:
        f.write(f"{item}\n")

moving_errored_files_real_data(error_files_real_data_directory, error_files_real_data_directories)