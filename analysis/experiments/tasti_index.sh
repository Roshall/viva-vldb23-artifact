#!/bin/bash

set -x
set -e

. viva_vars
queries=("deepface" "amsterdamdock" "dunk")

videos_prefix=${viva_root}/dataset
video_out=${viva_root}/data

pushd ${viva_root}

for q in ${queries[@]}; do
  video_in=${videos_prefix}/${q}/data
  [ -e ${video_out}/*.mp4 ] && rm ${video_out}/*.mp4
  cp ${video_in}/*.mp4 ${video_out}/
  $python viva/core/tasti.py -q ${q}
  mv ${video_out}/${q}_tasti_index.bin ${videos_prefix}/${q}/
done

popd

# Remove temporary Spark directories (excluding /tmp/spark-events)
source helper.sh
clean_spark
