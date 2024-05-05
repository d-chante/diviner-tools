#!/encs/bin/tcsh

#SBATCH --job-name=collect_aoi_data_diviner
#SBATCH --mail-type=ALL
#SBATCH --mail-user=chantelle.dubois@mail.concordia.ca
#SBATCH --chdir=/speed-scratch/d_chante/tmp
#SBATCH --time=3-00:00:00
#SBATCH --nodes=1 
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=15G

# Load anaconda module
module load anaconda3/2023.03/default

# Logging env
env

# Activate conda env
conda activate /speed-scratch/d_chante/env/dte

# Run preprocessing job
python3 /speed-scratch/d_chante/diviner-tools/scripts/collect_aoi_data.py \
    /speed-scratch/d_chante/diviner-tools/config/speed_cfg.yaml \
    /speed-scratch/d_chante/diviner-tools/config/aoi.yaml

# Deactivate the environment
conda deactivate

# End job
exit
