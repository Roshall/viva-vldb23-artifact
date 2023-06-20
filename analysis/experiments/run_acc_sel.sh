#!/bin/bash

source viva_vars
. helper.sh
hints_plan_path=viva/plans/experiment_hints.py

while getopts q:s:c:p:f:v:d:e:x:w: flag
do
    case "${flag}" in
        q) query_name=${OPTARG};;
        s) selectivity_fraction=${OPTARG};;
        c) canary_input=${OPTARG};;
        p) proxy_thresh=${OPTARG};;
        x) hints_plan=${OPTARG};;
    esac
done

echo "=====Experiment parameters====="
echo "Query: ${query_name}"
echo "Selectivity: ${selectivity_fraction}"
echo "Canary input: ${canary_input}"
echo "Proxy threshold: ${proxy_thresh}"
echo "Hints plan: ${hints_plan}"
echo "==============================="

# Change to home directory
pushd ${viva_root}
#===== Setup =====#
new_conf "${proxy_thresh}" "${script_dir}" "${query_name}" 0

# Copy hints plan as experiment_hints.py
cp ${script_dir}/hints_plans/${query_name}/${hints_plan} ${hints_plan_path}

# Set up input video, TASTI indexes, and similarity image.
# Assuming all are in resource/${query_name}
#cp data/${query_name}/${input_video} data/sample_vid.mp4 # Video
#cp data/${query_name}/${query_name}_tasti_index.bin.orig data/tasti_index.bin # TASTI
#cp data/${query_name}/${query_name}_similarity_img.png.orig data/similarity_img.png # Similarity

# Clear tmp/ and output/
#echo "Clearing tmp/ and output/"
#rm tmp/* output/*
. env_vars
#===== Run experiment =====#
$python calculate_metrics.py --query ${query_name} \
                             --selectivityfraction ${selectivity_fraction} \
                             --canary ${canary_input} \
                             -p

#===== Cleanup =====#
# Remove experiment_hints.py
rm ${hints_plan_path}

# Remove temporary Spark directories (excluding /tmp/spark-events)
clean_spark

# Copy the original conf back
cp conf.yml.orig conf.yml
popd
