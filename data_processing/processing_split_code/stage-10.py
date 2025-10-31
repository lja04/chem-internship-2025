import os
import csv
from pathlib import Path

# Paths to the directories and files
base_dir = "organised"
observed_crystals_path = "final_ranks_with_Sohncke.csv"
output_csv_path = "complete_crystal_info_fixed.csv"

def find_successful_structures_files(base_dir):
    """Find all successful-structures.csv files in subdirectories"""
    csv_files = []
    for root, dirs, files in os.walk(base_dir):
        if "successful-structures.csv" in files:
            csv_files.append(Path(root) / "successful-structures.csv")
    return csv_files

def read_observed_crystals(path):
    """Read the observed crystals data into a dictionary keyed by CSP_Match"""
    observed = {}
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            csp_match = row['CSP_Match']
            observed[csp_match] = row
    return observed

def process_files(csv_files, observed_data):
    """Process each CSV file and collect the required information"""
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
    """Write the collected information to a new CSV file"""
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

def main():
    csv_files = find_successful_structures_files(base_dir)
    print(f"Found {len(csv_files)} CSV files to process")
    
    observed_data = read_observed_crystals(observed_crystals_path)
    print(f"Loaded {len(observed_data)} observed crystal entries")
    
    results = process_files(csv_files, observed_data)
    
    write_output(results, output_csv_path)
    print(f"Results written to {output_csv_path}")

if __name__ == "__main__":
    main()