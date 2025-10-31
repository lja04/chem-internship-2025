import os
from pathlib import Path
from cspy.crystal import Crystal

# Define the root directories
source_root = '/scratch/la3g22/crystal-databases/imaginary-crystals'
target_root = '/lyceum/la3g22/summer-internship-2025/crystals'

# Walk through all directories and subdirectories
for root, dirs, files in os.walk(source_root):
    for file in files:
        if file.endswith('.res'):
            # Get the full source path
            source_path = os.path.join(root, file)
            
            # Extract the directory name (like 'bzcoct')
            relative_path = os.path.relpath(root, source_root)
            crystal_name = relative_path.split(os.sep)[0]  # Get the first directory name
            
            # Create the target directory structure
            target_dir = os.path.join(target_root, crystal_name, 'structure-files')
            os.makedirs(target_dir, exist_ok=True)
            
            # Create the target file path
            target_path = os.path.join(target_dir, file)
            
            try:
                # Convert the crystal to P1 symmetry
                crystal = Crystal.from_shelx_file(source_path)
                P1_crystal = crystal.as_primitive_P1()
                P1_crystal.to_shelx_file(target_path)
                
                print(f"Successfully converted: {source_path} -> {target_path}")
                
            except Exception as e:
                print(f"Error processing {source_path}: {str(e)}")
                continue

print("Conversion completed!")