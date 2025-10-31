import os
import csv
from pathlib import Path
import re
import pandas as pd

# Configuration
out_files_root = Path("testing")
initial_info_dir = Path("initial-crystal-info")
output_dir = Path("testing/organised")

# Only look for the KDE vibrational energy in .out files
kde_pattern = r"Epanechnikov KDE vibrational energy:\s+([-\d.]+)\s+kJ/mol"

def process_out_file(out_path):
    """Extract KDE vibrational energy from .out file"""
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

def process_crystal(crystal_name):
    """Process all files for one crystal using only KDE vibrational energy"""
    # Paths for this crystal
    crystal_out_dir = out_files_root / crystal_name
    initial_csv = initial_info_dir / f"{crystal_name}-structures.csv"
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

def main():
    # Find all crystal directories
    crystal_dirs = [d.name for d in out_files_root.iterdir() if d.is_dir()]
    
    for crystal_name in crystal_dirs:
        print(f"\nProcessing {crystal_name}...")
        process_crystal(crystal_name)
    
    print("\nProcessing complete!")

if __name__ == "__main__":
    main()