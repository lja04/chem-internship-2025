import csv

input_csv = "testing/complete_crystal_info_fixed.csv"
output_csv = "testing/energy_differences_sorted_3dp.csv"

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
    if 'initial_rank1' in entries and 'final_rank1' in entries and 'observed' in entries:
        initial_rank1 = entries['initial_rank1']
        final_rank1 = entries['final_rank1']
        
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
            except (ValueError, TypeError):
                # Skip if energy values are missing or invalid
                continue

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