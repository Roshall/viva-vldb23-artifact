#!/bin/bash

. viva_vars
useGPU=${1-0}

for q in ${queries[@]}; do
    bash query_sweep_${q}.sh "${useGPU}"
done

