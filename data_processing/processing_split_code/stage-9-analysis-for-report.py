import os
import pandas as pd

# Paths
observed_file = "final_ranks_with_Sohncke.csv"
results_dir = "testing/organised"
analysis_output_file = "testing/observed_crystals_analysis.csv"
raw_output_file = "testing/observed_crystals_raw_data.csv"
comparison_output_file = "testing/energy_comparison_data.csv"  # New output file
log_file = "testing/processing_log.txt"

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