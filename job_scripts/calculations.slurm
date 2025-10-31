#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=50:00:00
#SBATCH --partition=batch
#SBATCH --output=/lyceum/la3g22/summer-internship-2025/main-code/calculations_third_run_1.out


# Initialize conda for bash shell
source ~/.bashrc
module load conda
conda init bash
eval "$(conda shell.bash hook)"
conda activate cspy

# Run the Python script that generates the command files
python /lyceum/la3g22/summer-internship-2025/main-code/new-main-code.py || { echo "Main calculations failed"; exit 1; }

cd /lyceum/la3g22/summer-internship-2025/taskfarm-files

dmacrys_slurm_directory="/lyceum/la3g22/summer-internship-2025/slurm-files"

for slurm in "$dmacrys_slurm_directory"/*.slurm; do
    echo "Submitting $slurm"
    sbatch "$slurm" || { echo "Failed to submit $slurm"; exit 1; }
done