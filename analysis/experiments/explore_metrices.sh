#!/bin/bash

set -x
set -e
. viva_vars
. helper.sh

pushd ${viva_root}
. env_vars
new_conf "0.8" "${script_dir}" "angrybernie" 0 "False"

$python ${pyscript_root}/latency_estimator.py
sed -i "0,/gpu: False/s||gpu: True|" conf.yml
$python ${pyscript_root}/latency_estimator.py
#lat_file="resource/gpu_data_transfer.json"
#[ -e "${lat_file}" ] && rm "${lat_file}"
#$python ${pyscript_root}/transfer_benchmark.py

mv conf.yml.orig conf.yml

popd
