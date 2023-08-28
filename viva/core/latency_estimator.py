import json
import os
import sys
from os import path


basepath = path.dirname(__file__)
sys.path.append(path.abspath(path.join(basepath, '../../')))

from viva.core.utils import gen_input_df
from viva.utils.config import viva_setup
spark = viva_setup()
from viva.utils.config import ConfigManager
config = ConfigManager()

from timeit import default_timer as now
from typing import Dict

import pyspark.sql.dataframe as ppd

from viva.plans.profile_plan import ProfilePlan

# Produce batch_scale * batch_size inputs so that we get an accurate estimate
# of time per batch without initial startup overhead. We then divide the final
# end to end time by this value.
batch_scale = 10
batch_size = config.get_value('prediction', 'batch_size')
use_gpu = config.get_value('execution', 'gpu')
# batch_size = 16

overwrite_ops = False

GPU_NODES = [
    # 'actiondetect',
    # 'classification',
    # 'qclassification',
    'facedetect',
    'objectdetect',
    'objectdetect_large',
    'objectdetect_medium',
    'objectdetect_nano',
    'objectdetect_xlarge',
    # 'objecttrack',
    # 'proxyclassification',
    'img2vec',
    'emotiondetect',
    'emotiondetect_cascades',
    # 'deepfaceAge',
    # 'deepfaceGender',
    # 'deepfaceRace',
    # 'dfprefixembed',
    # 'deepfaceSuffixAge',
    # 'deepfaceSuffixGender',
    # 'deepfaceSuffixRace',
    'deepfaceEmotion',
    'deepfaceVerify',
]

def gen_lat_input_df(batch_size: int) -> ppd.DataFrame:
    df_i = gen_input_df(spark, config.get_value('storage', 'input'))

    return df_i.limit(batch_size * batch_scale)

def profile_node_latencies(df: ppd.DataFrame, prof_map: Dict[str, int], warmup: bool=False) ->  Dict[str, int]:
    # Any plan will do
    plan = ProfilePlan.all_plans[0]
    if warmup:
        print('Warm up run')

    # Add the first node twice since there is a startup overhead that will
    # produce misleading profiling results for it
    mod_plan = [plan[0]] + plan
    for i,node in enumerate(mod_plan):
        model = node.out_column

        # Skip; don't re-profile
        if model in prof_map:
            print(f'Skipping {model}.')
            continue

        if use_gpu and model not in GPU_NODES:
            prof_map[model] = 'NOT_GPU_OP' # hack
            continue
        print(f'Profiling {model}')

        # Only profile the op, not the filter
        df = node.apply_op(df)
        start = now()
        df.count()
        end = now()
        e2e = end - start

        # Ignore first node
        if i != 0 and not warmup:
            prof_map[model] = e2e / batch_scale
            print(f'Latency: {round(prof_map[model], 2)}')

        df = node.apply_filters(df)

    return prof_map

def profile_ops(output_name: str, batch_size: int, overwrite_ops: bool = False) -> None:
    prof_map = {}

    if not overwrite_ops:
        # Read in profiled ops if they already exist so we don't re-profile
        if os.path.exists(output_name):
            fd = open(output_name, 'r')
            prof_map = json.load(fd)
            fd.close()

    # warm up run
    _ = profile_node_latencies(gen_lat_input_df(32), prof_map, True)

    df = gen_lat_input_df(batch_size)
    prof_map = profile_node_latencies(df, prof_map)

    fd = open(output_name, 'w')
    json.dump(prof_map, fd, indent=4, sort_keys=True)
    fd.close()

if __name__ == '__main__':
    if use_gpu:
        fname = 'op_latency_bmarks_gpu.json'
    else:
        fname = 'op_latency_bmarks.json'

    default_output_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../resource/', fname)
    profile_ops(output_name=default_output_name, batch_size=batch_size, overwrite_ops=overwrite_ops)
