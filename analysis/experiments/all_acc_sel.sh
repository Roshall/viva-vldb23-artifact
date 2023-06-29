#!/bin/bash

#declare -A queries=( ["dunk"]="niceShoot.mp4")
selectivity_thr=0.1
proxy_thr=0.8
hints_plan="all_hints.py"

. viva_vars
for i in f; do
  q="${queries["$i"]}"
  bash run_acc_sel.sh -q "${q}" -s "${selectivity_thr}" -c "${viva_root}/dataset/${q}/canary/${canaries[$i]}" \
  -x "${hints_plan}" -p "${proxy_thr}" -n ${1}
done

echo "Done."