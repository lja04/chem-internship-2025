import pandas as pd
from pathlib import Path

# Configuration
final_ranks_file = Path("final_ranks_with_Sohncke.csv")
organised_dir = Path("results-from-autofree/organised-structure-csvs")
output_report = Path("observed_crystals_ranking_changes-polymorphs.txt")
previous_results_file = Path("dissertation-ranking-results.csv")

def find_crystal_info(csp_match):
    """Search for crystal in both successful and unsuccessful CSVs"""
    if pd.isna(csp_match) or not isinstance(csp_match, str):
        return None, 'not_found'
    
    try:
        base_name = csp_match.split('-')[0]
        successful_csv = organised_dir / base_name / "successful-structures.csv"
        unsuccessful_csv = organised_dir / base_name / "unsuccessful-structures.csv"
        
        if successful_csv.exists():
            df = pd.read_csv(successful_csv)
            match = df[df['Crystal ID'] == csp_match]
            if not match.empty:
                return match.iloc[0].to_dict(), 'successful'
        
        if unsuccessful_csv.exists():
            df = pd.read_csv(unsuccessful_csv)
            match = df[df['Crystal ID'] == csp_match]
            if not match.empty:
                return match.iloc[0].to_dict(), 'unsuccessful'
    except Exception as e:
        print(f"Error processing {csp_match}: {str(e)}")
    
    return None, 'not_found'

def generate_report():
    observed_df = pd.read_csv(final_ranks_file)
    previous_results = pd.read_csv(previous_results_file)
    
    results = []
    missing_crystals = []
    unsuccessful_crystals = []
    comparison_results = []
    
    # Counters for summary stats
    improved = 0
    dropped = 0
    unchanged = 0
    incomplete = 0
    comparison_improved = 0
    comparison_worsened = 0
    comparison_unchanged = 0
    
    for _, row in observed_df.iterrows():
        refcode = row['Refcode']
        csp_match = row['CSP_Match']
        
        if pd.isna(csp_match) or not isinstance(csp_match, str):
            missing_crystals.append(f"{refcode} (No CSP match provided)")
            continue
        
        crystal_info, status = find_crystal_info(csp_match)
        
        if status == 'successful':
            initial_rank = crystal_info.get('Initial Rank', 'NA')
            final_rank = crystal_info.get('Final Rank', 'NA')
            rank_change = crystal_info.get('Rank Change', 'NA')
            total_energy = crystal_info.get('Total Final Energy (kJ/mol)', 'NA')
            
            if pd.isna(rank_change):
                rank_msg = "Ranking data incomplete"
                incomplete += 1
            else:
                if rank_change > 0:
                    rank_msg = f"Improved by {abs(rank_change)} positions"
                    improved += 1
                elif rank_change < 0:
                    rank_msg = f"Dropped by {abs(rank_change)} positions"
                    dropped += 1
                else:
                    rank_msg = "No change in ranking"
                    unchanged += 1
            
            # Compare with previous results if available
            prev_result = previous_results[previous_results['Refcode'] == refcode]
            if not prev_result.empty:
                prev_rank = prev_result.iloc[0]['New Rank']
                prev_change = prev_result.iloc[0]['Rank Change']
                prev_energy = prev_result.iloc[0]['Energy Difference (kJ mol⁻¹)']
                
                if isinstance(final_rank, (int, float)) and isinstance(prev_rank, (int, float)):
                    comparison_change = prev_rank - final_rank
                    if comparison_change > 0:
                        comparison_msg = f"Improved by {comparison_change} positions compared to previous"
                        comparison_improved += 1
                    elif comparison_change < 0:
                        comparison_msg = f"Worsened by {abs(comparison_change)} positions compared to previous"
                        comparison_worsened += 1
                    else:
                        comparison_msg = "No change compared to previous"
                        comparison_unchanged += 1
                else:
                    comparison_msg = "Cannot compare - missing rank data"
                
                comparison_results.append(
                    f"{refcode} ({csp_match}):\n"
                    f"  Previous Rank: {prev_rank} (Change: {prev_change}, Energy: {prev_energy})\n"
                    f"  Current Final Rank: {final_rank} (Change: {rank_msg}, Energy: {total_energy})\n"
                    f"  Comparison: {comparison_msg}\n"
                )
            
            results.append(
                f"{refcode} ({csp_match}):\n"
                f"  Initial Rank: {initial_rank}\n"
                f"  Final Rank: {final_rank}\n"
                f"  Rank Change: {rank_msg}\n"
                f"  Total Final Energy: {total_energy} kJ/mol\n"
            )
        elif status == 'unsuccessful':
            unsuccessful_crystals.append(f"{refcode} ({csp_match})")
        else:
            missing_crystals.append(f"{refcode} ({csp_match})")
    
    # Write the report
    with open(output_report, 'w') as f:
        f.write("=== Ranking Changes for Observed Crystals ===\n\n")
        
        # Summary statistics
        f.write("=== SUMMARY ===\n")
        f.write("Current results:\n")
        f.write(f"Improved in ranking: {improved}\n")
        f.write(f"Dropped in ranking: {dropped}\n")
        f.write(f"Unchanged ranking: {unchanged}\n")
        f.write(f"Incomplete ranking data: {incomplete}\n")
        f.write(f"Unsuccessful runs: {len(unsuccessful_crystals)}\n")
        f.write(f"Not found in outputs: {len(missing_crystals)}\n\n")
        
        f.write("\nComparison with previous results:\n")
        f.write(f"Improved compared to previous: {comparison_improved}\n")
        f.write(f"Worsened compared to previous: {comparison_worsened}\n")
        f.write(f"Unchanged compared to previous: {comparison_unchanged}\n\n")
        
        if results:
            f.write("Crystals with successful runs and ranking changes:\n")
            f.write("---------------------------------------------\n")
            f.write("\n".join(results))
            f.write("\n\n")
        
        if comparison_results:
            f.write("\nComparison with previous results (for matched crystals):\n")
            f.write("----------------------------------------------------\n")
            f.write("\n".join(comparison_results))
            f.write("\n\n")
        
        if unsuccessful_crystals:
            f.write("\nThese crystals haven't had a successful run yet:\n")
            f.write("---------------------------------------------\n")
            f.write("\n".join(unsuccessful_crystals))
            f.write("\n\n")
        
        if missing_crystals:
            f.write("\nThese crystals were not found in any output files:\n")
            f.write("---------------------------------------------\n")
            f.write("\n".join(missing_crystals))
            f.write("\n")
        
        f.write("\n=== End of Report ===")

    print(f"Report generated at {output_report}")

if __name__ == "__main__":
    generate_report()