. viva_vars

pushd ${viva_root}

sed -i "s/gpu:.*/gpu: False/g" conf.yml
$python ${pyscript_root}/latency_estimator.py
sed -i "s/gpu:.*/gpu: False/g" conf.yml
$python ${pyscript_root}/latency_estimator.py
lat_file=data/gpu_data_transfer.json
[ -e ${lat_file} ] && rm lat_file
$python ${pyscript_root}/transfer_benchmark.py

popd