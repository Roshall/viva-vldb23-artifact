#!/bin/bash

set -x
. viva_vars
source helper.sh

hints_plan_path=viva/plans/experiment_hints.py

while getopts q:s:c:p:f:v:d:e:x:w:n: flag
do
    case "${flag}" in
        q) query_name=${OPTARG};;
        s) selectivity_fraction=${OPTARG};;
        c) canary_input=${OPTARG};;
        p) proxy_thresh=${OPTARG};;
        f) f1_thresh=${OPTARG};;
        v) input_video=${OPTARG};;
        d) viva_root=${OPTARG};;
        e) costminmax=${OPTARG};;
        x) hints_plan=${OPTARG};;
        w) do_warmup=${OPTARG};;
        n) ex_id=${OPTARG};;
      *) echo "invalid argument ${OPTARG}";;
    esac
done

echo "=====Experiment parameters====="
echo "Query: ${query_name}"
echo "Input video: ${input_video}"
echo "Selectivity: ${selectivity_fraction}"
echo "Canary input: ${canary_input}"
echo "Proxy threshold: ${proxy_thresh}"
echo "F1 threshold: ${f1_thresh}"
echo "Cost optimizer min/max: ${costminmax}"
echo "Hints plan: ${hints_plan}"
echo "Do warmup: ${do_warmup}"
echo "==============================="

# Change to home directory
pushd "${viva_root}" || exit
#===== Setup =====#
# Save the original conf if it has not been saved before.
# Tries to prevent incomplete runs from overwriting confs, but this can be disabled
if [ ! -f "conf.yml.orig" ]; then
    echo "Copying conf.yml to conf.yml.orig"
    mv conf.yml conf.yml.orig
fi

# Generate new conf based on experiment input
sed "s|<PROXY_THRESH>|${proxy_thresh}|g" "${script_dir}"/conf.yml.templ |\
sed "s|input:.*|input: 'dataset/${query_name}/data/'|" |\
sed "s|output:.*|output: 'output${ex_id}/'|" |\
sed "s|tmp:.*|tmp: 'tmp${ex_id}/'|" > conf.yml

# Copy hints plan as experiment_hints.py
cp "${script_dir}"/hints_plans/"${query_name}"/"${hints_plan}" ${hints_plan_path}

# If this is a warmup run, first clear tmp/ and output/. Otherwise, do not
if [ "${do_warmup}" == "1" ]; then
#    echo "clean all mp4 files in data"
#    find resource/ -maxdepth 1 -iname '*.mp4' -delete
#  mv -b output/"${query_name}"/ "${query_name}"
    echo "Clearing tmp/ and output/"
    rm -rf "tmp${ex_id}/*" "output${ex_id}/*"
else
    echo "Leaving tmp/ and output/ intact"
fi

# Set up input video, TASTI indexes, and similarity image.
# Assuming all are in resource/${query_name}


#cp dataset/"${query_name}"/resource/"${input_video}" resource/sample_vid.mp4
#cp dataset/"${query_name}"/"${query_name}"_tasti_index.bin resource/tasti_index.bin # TASTI
#cp dataset/"${query_name}"/"${query_name}"_similarity_img.png resource/similarity_img.png # Similarity

#===== Run experiment =====#
# If do_warmup is 1, run once with minimal parameters to warm up the ingest
. env_vars
export CUDA_VISIBLE_DEVICES=${ex_id}
if [ "${do_warmup}" == "1" ]; then
    $python run_query.py --logging \
                         --query "${query_name}" \
                         --canary "${canary_input}" \
                         --ingestwarmup
fi

logging_suffix="${input_video},${selectivity_fraction},${canary_input},${proxy_thresh},${f1_thresh},${costminmax},${hints_plan}"
$python run_query.py --logging "${logging_suffix}" \
                     --query "${query_name}" \
                     --selectivityfraction "${selectivity_fraction}" \
                     --f1thresh "${f1_thresh}" \
                     --costminmax "${costminmax}" \
                     --canary "${canary_input}" \
                     --logname "${query_name}-${hints_plan%.*}.log"

#===== Cleanup =====#
# move video back

# Remove experiment_hints.py
rm ${hints_plan_path}

# Remove temporary Spark directories (excluding /tmp/spark-events)
find /tmp/spark* -mindepth 1 ! -regex '^/tmp/spark-events\(/.*\)?' -delete
# Remove temporary Spark directories (excluding /tmp/spark-events)
clean_spark
# Copy the original conf back
cp conf.yml.orig conf.yml
popd || exit
