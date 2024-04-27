#!/bin/bash

eval "$(conda shell.bash hook)"

conda activate diviner

python3 ~/diviner-tools/scripts/create_aoi_indices.py \
    /mnt/esthar/diviner_data/data/aoi/aoi.db

conda deactivate