#!/bin/bash

. viva_vars

for q in ${queries[@]}; do
    bash query_sweep_${q}.sh
done

