import os
import shutil
from collections import defaultdict

def analyze_folder_structure(root_dir):
    folder_stats = {}
    count_distribution = defaultdict(int)
    folders_by_count = defaultdict(list)

    for root, dirs, files in os.walk(root_dir):
        file_count = len(files)
        folder_stats[root] = file_count
        count_distribution[file_count] += 1
        folders_by_count[file_count].append(root)
    
    return folder_stats, count_distribution, folders_by_count

def save_folders_by_count(folders_by_count):
    output_dir = "folder_counts"
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\nSaving folder lists to {output_dir}/ directory:")
    
    for count, folders in folders_by_count.items():
        filename = os.path.join(output_dir, f"folders_with_{count}_files.txt")
        with open(filename, 'w') as f:
            f.write("\n".join(folders))
        print(f"- {filename} ({len(folders)} folders)")

def move_two_file_folders(folders_by_count, target_base):
    two_file_folders = folders_by_count.get(2, [])
    if not two_file_folders:
        print("\nNo folders with exactly 2 files found.")
        return
    
    target_dir = os.path.join(target_base, "unrun-dmacrys")
    os.makedirs(target_dir, exist_ok=True)
    
    print(f"\nMoving {len(two_file_folders)} folders with 2 files to {target_dir}")
    
    moved_count = 0
    for src_path in two_file_folders:
        try:
            # Preserve the last two directory levels (molecule/crystal)
            rel_path = os.path.join(*src_path.split(os.sep)[-2:])
            dest_path = os.path.join(target_dir, rel_path)
            
            # Create parent directories if needed
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            shutil.move(src_path, dest_path)
            moved_count += 1
            print(f"Moved: {rel_path}")
        except Exception as e:
            print(f"Failed to move {src_path}: {str(e)}")
    
    print(f"\nSuccessfully moved {moved_count} folders")
    if moved_count < len(two_file_folders):
        print(f"Failed to move {len(two_file_folders) - moved_count} folders")

def print_summary(folder_stats, count_distribution):
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

if __name__ == "__main__":
    root_directory = 'testing'
    error_directory = 'testing-with-errors'
    
    print(f"Analyzing folder structure for: {root_directory}")
    folder_stats, count_distribution, folders_by_count = analyze_folder_structure(root_directory)
    
    print_summary(folder_stats, count_distribution)
    move_two_file_folders(folders_by_count, error_directory)