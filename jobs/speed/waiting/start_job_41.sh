#!/encs/bin/tcsh

#SBATCH --job-name=41_diviner_preprocess
#SBATCH --mail-type=ALL
#SBATCH --mail-user=chantelle.dubois@mail.concordia.ca
#SBATCH --chdir=/speed-scratch/d_chante/tmp
#SBATCH --time=5-00:00:00
#SBATCH --nodes=1 
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=15G

# Load anaconda module
module load anaconda3/default

# Activate conda env
conda activate /speed-scratch/d_chante/env/dte

# Run preprocessing job
python3 /speed-scratch/d_chante/diviner-tools/scripts/data_preprocess.py \
    /speed-scratch/d_chante/diviner-tools/config/speed_cfg.yaml \
    /speed-scratch/d_chante/diviner-tools/support/other/zip_urls.txt \
    41

# Deactivate the environment
conda deactivate

# End job
exit
