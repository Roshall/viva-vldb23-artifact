#!/bin/bash

set -x
set -e
. viva_vars

mv ${viva_root}/conf.yml ${viva_root}/conf.yml.orig
sed "s/gpu:.*/gpu: False/" conf.yml.templ > ${viva_root}/conf.yml

pushd ${viva_root}

$python ${pyscript_root}/latency_estimator.py
sed -i "s/gpu:.*/gpu: True/" conf.yml
$python ${pyscript_root}/latency_estimator.py
lat_file="data/gpu_data_transfer.json"
[ -e "${lat_file}" ] && rm "${lat_file}"
$python ${pyscript_root}/transfer_benchmark.py

mv conf.yml.orig conf.yml

popd