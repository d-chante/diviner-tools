#!/bin/bash

#SBATCH --job-name=setup_env
#SBATCH --mail-type=ALL
#SBATCH --mail-user=chantelle.dubois@mail.concordia.ca
#SBATCH --time=00:30:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1G

# Load conda module
module load anaconda/2020.07  

# Set path to conda env
ENV_PATH=/speed-scratch/d_chante/dte

# Create conda env
conda create --prefix $ENV_PATH python=3.8  

# Activate conda env
source activate $ENV_PATH

# Install pcakages from requirements.txt
pip install -r /speed-scratch/d_chante/diviner-tools/support/other/requirements.txt

# Deactivate the environment
conda deactivate

# End job
exit