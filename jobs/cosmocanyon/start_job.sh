#!/bin/bash

# Capture the job_id argument
job_id=$1

python3 ~/diviner-tools/scripts/data_preprocess.py \
    ~/diviner-tools/config/cosmocanyon_cfg.yaml \
    ~/diviner-tools/support/other/zip_urls.txt \
    ${job_id}