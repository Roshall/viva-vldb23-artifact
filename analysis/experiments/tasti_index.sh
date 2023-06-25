#!/bin/bash

set -x
#set -e

. viva_vars
. helper.sh
proxy_thresh=0.8

pushd ${viva_root}
. env_vars
for i in b; do
  q="${queries[$i]}"
  new_conf ${proxy_thresh} ${script_dir} ${q} 0
  output_prefix=${viva_root}/dataset/${q}/data
  cp viva/plans/${q}_tasti_plan.py viva/plans/tasti_plan.py
  $python viva/core/tasti.py -q ${q} -p ${output_prefix}
done

cp conf.yml.orig conf.yml
# Remove temporary Spark directories (excluding /tmp/spark-events)
#clean_spark

popd
