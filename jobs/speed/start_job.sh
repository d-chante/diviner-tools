#!/encs/bin/tcsh

# Capture the job_id argument
set job_id = $1

# Generate job name
set job_name = "${job_id}_diviner_preprocess"

# Use --export to pass the JOB_ID variable to the script environment and set the job name
sbatch --job-name=$job_name --export=JOB_ID=$job_id speed_job.sh