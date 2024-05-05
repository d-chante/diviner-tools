#!/encs/bin/tcsh

#SBATCH --job-name=create_aoi_indices_diviner
#SBATCH --mail-type=ALL
#SBATCH --mail-user=chantelle.dubois@mail.concordia.ca
#SBATCH --chdir=/speed-scratch/d_chante/tmp
#SBATCH --time=3-00:00:00
#SBATCH --nodes=1 
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=11
#SBATCH --mem=100G

# Load anaconda module
module load anaconda3/2023.03/default

# Logging env
env

# Activate conda env
conda activate /speed-scratch/d_chante/env/dte

# Run preprocessing job
python3 /speed-scratch/d_chante/diviner-tools/scripts/create_aoi_indices.py \
    /speed-scratch/d_chante/diviner_data/data/aoi/aoi.db

# Deactivate the environment
conda deactivate

# End job
exit
