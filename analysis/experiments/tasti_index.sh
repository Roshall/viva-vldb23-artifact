#!/bin/bash

set -x
#set -e

. viva_vars
. helper.sh
queries=("angrybernie" "dunk" "amsterdamdock")
proxy_thresh=0.8

pushd ${viva_root}
. env_vars
for q in ${queries[@]}; do
  new_conf ${proxy_thresh} ${script_dir} ${q} 2
  output_prefix=${viva_root}/dataset/${q}/data
  cp viva/plans/${q}_tasti_plan.py viva/plans/tasti_plan.py
#  find data/-maxdepth 1 -iname *.mp4 -delete
#  cp ${video_in}/*.mp4 ${video_out}/
  $python viva/core/tasti.py -q ${q} -p ${output_prefix}
#  mv ${video_out}/${q}_tasti_index.bin ${videos_prefix}/${q}/
done

cp conf.yml.orig conf.yml
# Remove temporary Spark directories (excluding /tmp/spark-events)
clean_spark

popd
