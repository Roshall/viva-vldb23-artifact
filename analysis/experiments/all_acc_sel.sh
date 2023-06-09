#!/bin/bash

#declare -A queries=( ["dunk"]="niceShoot.mp4")
selectivity_thr=0.1
proxy_thr=0.8
hints_plan="all_hints.py"
canary_prefix="../dataset"
for i in {0..3}; do
  bash run_acc_sel.sh -q "${queries[$i]}" -s "${selectivity_thr}" -c "${canary_prefix}/${q}/canary/${canaries[$i]}" \
  -x "${hints_plan}"
done

echo "Done."