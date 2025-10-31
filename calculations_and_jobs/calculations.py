import os
import shutil
import csv
import subprocess
from pathlib import Path
import math

# 

crystal_base_directory = "crystals"
core_files_base_directory = 'core-files/'
multi_moles_directory = 'all-multi-moles-xyzs-files'
dmacrys_taskfarm_txt_output_location = 'dmacrys-files'
slurm_output_location = 'slurm-files'

standard_core_files = [
    os.path.join(core_files_base_directory, "bondlengths"),
    os.path.join(core_files_base_directory, "fit.pots")]

autold_script = os.path.join(core_files_base_directory, "AutoLD.py")


def res_file_to_res_folder(base_directory):
    
    for crystal_folder in os.listdir(base_directory):
        crystal_path = os.path.join(base_directory, crystal_folder)

        if not os.path.isdir(crystal_path):
            print("Skipping non-directory:", crystal_path)
            continue

        structure_path = os.path.join(crystal_path, "structure-files")

        if not os.path.exists(structure_path):
            print("Skipping missing structure files directory:", structure_path)
            continue

        for res_file in os.listdir(structure_path):
            res_file_path = os.path.join(structure_path, res_file)

            if os.path.isfile(res_file_path) and res_file.endswith(".res"):
                res_folder_name = res_file[:-4]

                new_folder_path = os.path.join(structure_path, res_folder_name)
                os.makedirs(new_folder_path, exist_ok=True)

                new_res_file_path = os.path.join(new_folder_path, res_file)
                shutil.move(res_file_path, new_res_file_path)

                print(f"Moved {res_file} to {new_folder_path}")

def finalising_res_folders(base_directory):

    for crystal_folder in os.listdir(base_directory):
        crystal_path = os.path.join(base_directory, crystal_folder)

        if not os.path.isdir(crystal_path):
                print("Skipping non-directory:", crystal_path)
                continue

        structure_path = os.path.join(crystal_path, "structure-files")

        if not os.path.exists(structure_path):
                print("Skipping missing structure files directory:", structure_path)
                continue
        
        for res_folder in os.listdir(structure_path):
            res_folder_path = os.path.join(structure_path, res_folder)

            if not os.path.isdir(res_folder_path):
                print("Skipping directory due to false path OR no RES file detected:",res_folder_path)
                continue

            for standard_file in standard_core_files:
                 if os.path.exists(standard_file):
                      shutil.copy2(standard_file, res_folder_path)
                      print(f"Copied {standard_file} to {res_folder_path}")
            
            for ext in ['.mols', '.dma']:
                 required_file = os.path.join(multi_moles_directory, f"{res_folder.split('-')[0]}{ext}")
                 if os.path.exists(required_file):
                      shutil.copy2(required_file, res_folder_path)
                      print(f"Copied {os.path.basename(required_file)} to {res_folder_path}")
                 else:
                      print(f"Warning: {required_file} not found for {res_folder}")

def fort_file_creator(base_directory):
     
     for crystal_folder in os.listdir(base_directory):
        crystal_path = os.path.join(base_directory, crystal_folder)

        if not os.path.isdir(crystal_path):
            print("Skipping non-directory:", crystal_path)
            continue

        structure_path = os.path.join(crystal_path, "structure-files")

        for res_folder in os.listdir(structure_path):
             res_folder_path = os.path.join(structure_path, res_folder)
             if os.path.isdir(res_folder_path):
                  res_filename = res_folder
                  crystal_name = res_filename.split('-')[0]
                  content = f"""I
{res_filename}.res
bondlengths
4.0000
n
n
f
n
0
y
{crystal_name}.dma
y
{crystal_name}.mols
n
y
fit.pots
"""
                  
                  with open(os.path.join(res_folder_path, "fort.22"), "w") as f:
                       f.write(content)
                       
        print(f"Created fort.22 files in all folders")
               
def run_neighcrys(base_directory):
     
     for crystal_folder in os.listdir(base_directory):
        crystal_path = os.path.join(base_directory, crystal_folder)

        if not os.path.isdir(crystal_path):
            print("Skipping non-directory:", crystal_path)
            continue

        structure_path = os.path.join(crystal_path, "structure-files")

        if not os.path.exists(structure_path):
            print("Skipping missing structure files directory:", structure_path)
            continue
             
        for res_folder in os.listdir(structure_path):
            res_folder_path = os.path.join(structure_path, res_folder)
            fort_22_path = os.path.join(res_folder_path, "fort.22")

            if os.path.exists(fort_22_path):
                    print(f"Processing: {os.path.basename(res_folder)}")

                    try:
                         run_process = subprocess.Popen(
                              ["neighcrys", "fort.22"],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True,
                                cwd=res_folder_path
                         )

                         stdout, stderr = run_process.communicate(input="\n\n")
                         print(f"Completed: {os.path.basename(res_folder)}\n")
                         dmain_file = os.path.join(res_folder_path, res_folder + ".res.dmain")

                         if not os.path.exists(dmain_file):
                                print(f"Warning: No .dmain file created in {os.path.basename(res_folder)}")

                    except subprocess.CalledProcessError as e:
                            print(f"Error in {res_folder}:\n{e.stderr}")
                
            else:
                    print(f"Skipping {os.path.basename(res_folder)}: fort.22 not found")
                        
     print("All folders have successfully run NEIGHCRYS")

def remove_spli_lines_from_all_files(base_directory):
     
     for crystal_folder in os.listdir(base_directory):
        crystal_path = os.path.join(base_directory, crystal_folder)

        if not os.path.isdir(crystal_path):
            print("Skipping non-directory:", crystal_path)
            continue

        structure_path = os.path.join(crystal_path, "structure-files")

        if not os.path.exists(structure_path):
            print("Skipping missing structure files directory:", structure_path)
            continue

        for res_folder in os.listdir(structure_path):
            res_folder_path = os.path.join(structure_path, res_folder)

            if os.path.isdir(res_folder_path):
                 dmain_file_path = os.path.join(res_folder_path, res_folder + ".res.dmain")

                 if os.path.exists(dmain_file_path):

                      try:
                           with open(dmain_file_path, 'r') as file:
                                lines = file.readlines()
                           filtered_lines = [line for line in lines if not line.startswith("SPLI")]

                           with open(dmain_file_path, 'w') as file:
                                file.writelines(filtered_lines)
                           print(f"Processed: {dmain_file_path}")

                      except Exception as e:
                           print(f"Error processing {dmain_file_path}: {e}")

                 else:
                    print(f"Skipping {res_folder}: .res.dmain file not found")

     print("All .res.dmain files processed")
     
def removing_used_files(base_directory):
        
        for crystal_folder in os.listdir(base_directory):
            crystal_path = os.path.join(base_directory, crystal_folder)
            
            if not os.path.isdir(crystal_path):
                    print("Skipping non-directory:", crystal_path)
                    continue
            
            structure_path = os.path.join(crystal_path, "structure-files")
    
            if not os.path.exists(structure_path):
                    print("Skipping missing structure files directory:", structure_path)
                    continue
            
            for res_folder in os.listdir(structure_path):
                res_folder_path = os.path.join(structure_path, res_folder)
    
                if os.path.isdir(res_folder_path):
                    fort_22_path = os.path.join(res_folder_path, "fort.22")
                    res_file_path = os.path.join(res_folder_path, f"{res_folder}.res")
                    dma_path = os.path.join(res_folder_path, f"{crystal_folder}.dma")
                    mols_path = os.path.join(res_folder_path, f"{crystal_folder}.mols")
                    bondlengths_path = os.path.join(res_folder_path, "bondlengths")
                    fit_pots_path = os.path.join(res_folder_path, "fit.pots")
                    
                    if os.path.exists(fort_22_path):
                        os.remove(fort_22_path)
                        print(f"Removed {fort_22_path}")
                    
                    else:
                        print(f"Skipping {res_folder}: fort.22 not found")
                        
                    if os.path.exists(res_file_path):
                        os.remove(res_file_path)
                        print(f"Removed {res_file_path}")
                    
                    else:
                        print(f"Skipping {res_folder}: .res file not found")
                        
                    if os.path.exists(dma_path):
                        os.remove(dma_path)
                        print(f"Removed {dma_path}")
                        
                    else:
                        print(f"Skipping {res_folder}: .dma file not found")
                        
                    if os.path.exists(mols_path):
                        os.remove(mols_path)
                        print(f"Removed {mols_path}")
                        
                    else:
                        print(f"Skipping {res_folder}: .mols file not found")
                        
                    if os.path.exists(bondlengths_path):
                        os.remove(bondlengths_path)
                        print(f"Removed {bondlengths_path}")
                        
                    else:
                        print(f"Skipping {res_folder}: bondlengths file not found")
                        
                    if os.path.exists(fit_pots_path):
                        os.remove(fit_pots_path)
                        print(f"Removed {fit_pots_path}")
                        
                    else:
                        print(f"Skipping {res_folder}: fit.pots file not found")
                        
                else:
                    print(f"Skipping {res_folder}: Not a directory")
                    
def copying_auto_ld_script(base_directory, script):
     
     for crystal_folder in os.listdir(base_directory):
          crystal_path = os.path.join(base_directory, crystal_folder)
          
          if not os.path.isdir(crystal_path):
                print("Skipping non-directory:", crystal_path)
                continue
          
          structure_path = os.path.join(crystal_path, "structure-files")

          if not os.path.exists(structure_path):
                print("Skipping missing structure files directory:", structure_path)
                continue
          
          for res_folder in Path(structure_path).iterdir():
               
               if res_folder.is_dir():
                    print(f"Copying script to: {res_folder.name}")

                    try:
                        shutil.copy(autold_script, res_folder)
                        print(f"Copied {Path(autold_script).name} to {res_folder.name}")

                    except Exception as e:
                        print(f"Error copying {Path(script).name} to {res_folder.name}: {e}")
               else:
                    print(f"Skipping {res_folder.name}: Not a directory")

     print("AutoLD.py has been copied into all folders")
               
def run_autold(base_directory):
     
    for crystal_folder in os.listdir(base_directory):
        crystal_path = os.path.join(base_directory, crystal_folder)

        if not os.path.isdir(crystal_path):
            print("Skipping non-directory:", crystal_path)
            continue

        structure_path = os.path.join(crystal_path, "structure-files")

        if not os.path.exists(structure_path):
            print("Skipping missing structure files directory:", structure_path)
            continue

        for res_folder in os.listdir(structure_path):
            res_folder_path = os.path.join(structure_path, res_folder)

            if os.path.isdir(res_folder_path):
                 autold_path = os.path.join(res_folder_path, "AutoLD.py")

                 print("AUTOLD PATH:", autold_path)

                 if os.path.exists(autold_path):
                      print(f"Processing: {os.path.basename(res_folder)}")

                      try:
                          result = subprocess.run(
                            ["python", str(autold_path), "-k", "0.08"],
                            cwd=res_folder_path,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True
                        )
                          
                      except Exception as e:
                          print(f"Error running AutoLD.py in {res_folder}: {e}")
                    
                 else:
                      print(f"Skipping {os.path.basename(res_folder)}: AutoLD.py not found")
            else:
                print(f"Skipping {os.path.basename(res_folder)}: Not a directory")
    
    print("AutoLD.py execution completed")
    
def removing_used_scripts(base_directory):
        
        for crystal_folder in os.listdir(base_directory):
            crystal_path = os.path.join(base_directory, crystal_folder)
    
            if not os.path.isdir(crystal_path):
                print("Skipping non-directory:", crystal_path)
                continue
    
            structure_path = os.path.join(crystal_path, "structure-files")
    
            if not os.path.exists(structure_path):
                print("Skipping missing structure files directory:", structure_path)
                continue
    
            for res_folder in os.listdir(structure_path):
                res_folder_path = os.path.join(structure_path, res_folder)
    
                if os.path.isdir(res_folder_path):
                    autold_script_path = os.path.join(res_folder_path, "AutoLD.py")
    
                    if os.path.exists(autold_script_path):
                        os.remove(autold_script_path)
                        print(f"Removed {autold_script_path}")

                else:
                    print(f"Skipping {res_folder}: Not a directory")
                      
def obtaining_dmaout_command_lines(base_directory):
    
    dmain_commands_list = []
    
    for crystal_folder in os.listdir(base_directory):
        crystal_path = os.path.join(base_directory, crystal_folder)

        if not os.path.isdir(crystal_path):
            print("Skipping non-directory:", crystal_path)
            continue

        structure_path = os.path.join(crystal_path, "structure-files")

        if not os.path.exists(structure_path):
            print("Skipping missing structure files directory:", structure_path)
            continue

        for res_folder in os.listdir(structure_path):
            res_folder_path = os.path.join(structure_path, res_folder)
            
            if os.path.isdir(res_folder_path):
                
                for dmain_file in os.listdir(res_folder_path):
                    
                    if dmain_file.endswith(".dmain"):
                        base_dmain_file_name = os.path.splitext(dmain_file)[0]
                        dmaout_command = f"cd {res_folder_path} ; dmacrys2.2.1 < {dmain_file} > {base_dmain_file_name}.dmaout\n"
                        dmain_commands_list.append(dmaout_command)
                        print(f"Added command for {dmain_file} in {res_folder_path}")          
                
            else:
                print(f"Skipping {res_folder}: Not a directory")
                
    return dmain_commands_list
        
def preparing_taskfarm_files(commands, txt_output_location, slurm_output_location):
    
    maximum_commands_per_file = 3000
    number_of_files = math.ceil(len(commands) / maximum_commands_per_file)
    number_of_commands_per_file = (len(commands) // number_of_files)
    print(f"Total commands: {len(commands)}, Number of files to create: {number_of_files}, Number of commands per file: {number_of_commands_per_file}")
    
    for i in range(number_of_files):
        start = i * number_of_commands_per_file
        end = start + number_of_commands_per_file
        split_dmacrys_commands = commands[start:end]
        split_dmacrys_path = f"{txt_output_location}/dmacrys_taskfarm_{i+1}.txt"
        
        with open(split_dmacrys_path, 'w') as f:
            f.writelines(split_dmacrys_commands)
            
        dmacrys_slurm_path = f"{slurm_output_location}/dmacrys_taskfarm_{i+1}.slurm"
        slurm_content = f"""#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=40
#SBATCH --time=48:00:00
#SBATCH --partition=batch
#SBATCH --output=%j_dmacrys_taskfarm_{i+1}.out

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

staskfarm {split_dmacrys_path}"""                      
        
        with open(dmacrys_slurm_path, 'w') as f:
            f.writelines(slurm_content)
            
        print(f"Written {len(split_dmacrys_commands)} commands to {split_dmacrys_path}")
        print(f"Created SLURM file: {dmacrys_slurm_path}")

if __name__ == "__main__":

    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ START OF SCRIPT ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    print("\nSTAGE 1: Creating RES file folders.")
    res_file_to_res_folder(crystal_base_directory)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    print("\nSTAGE 2: Adding all the required files into each folder.")
    finalising_res_folders(crystal_base_directory)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    print("\nSTAGE 3: Creating fort.22 files into each folder.")
    fort_file_creator(crystal_base_directory)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    print("\nSTAGE 4: Running neighcrys on each fort.22 file to generate the .dmain files.")
    run_neighcrys(crystal_base_directory)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    print("\nSTAGE 5: Removing SPLI lines from all .dmain files.")
    remove_spli_lines_from_all_files(crystal_base_directory)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    print("\nSTAGE 6: Removing used files from each folder.")
    removing_used_files(crystal_base_directory)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    print("\nSTAGE 7: Copying AutoLD.py scripts into each folder.")
    copying_auto_ld_script(crystal_base_directory, autold_script)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    print("\nSTAGE 8: Running AutoLD.py in each folder to generate the .ld files.")
    run_autold(crystal_base_directory)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    print("\nSTAGE 9: Removing used scripts from each folder.")
    removing_used_scripts(crystal_base_directory)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    print("\nSTAGE 10: Obtaining dmacrys command lines for each .dmain file.")
    dmain_commands_list = obtaining_dmaout_command_lines(crystal_base_directory)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    print("\nSTAGE 11: Preparing taskfarm files with dmacrys commands and creating slurm files.")
    preparing_taskfarm_files(dmain_commands_list, dmacrys_taskfarm_txt_output_location, slurm_output_location)
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ [COMPLETED] ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ END OF SCRIPT ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

