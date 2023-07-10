import argparse
import os
import pathlib
import random

from viva.utils.config import viva_setup, ConfigManager

spark = viva_setup()
config = ConfigManager()

from viva.core.session import VIVA
from viva.core.optimizer import Optimizer

import numpy as np

def canary_frame_ids(plan, session, df, logs, canary_name):
    plan_c = plan.all_plans[0]
    df_o = session.run(df, plan_c, {}, canary_name)
    return [r.id for r in df_o.select(df_o.id).collect()]


def calculate_f1_scores(opt, file_name):

    # ignore returned best plan just need computation
    epoch = 400
    stat_bin = np.empty(epoch+1)
    stat_bin[-1] = len(opt.plans)
    for i in range(epoch):
        opt.get_optimal_plan()
        random.shuffle(opt.plans)
        stat_bin[i] = opt.skip_candidate
        # print('='*20, "skip plans avg:", opt.skip_candidate)
        opt.skip_candidate = 0
        opt.reset_cache()
    file_path = os.path.join(config.get_value("logging", 'output'), file_name)
    np.save(file_path, stat_bin)
    print("="*20, "skip plans avg: ", stat_bin[:-1].mean()/stat_bin[-1], f"over ({stat_bin[-1]})")
        # log_times.update(opt.log_times)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--selectivityfraction', '-s', type=float,
                        required=False, default=0.1,
                        dest='selectivityfraction',
                        help='Fraction of frames to estimate selectivity over (Default: 0.1). 0 to disable estimating.')
    parser.add_argument('--selectivityrandom', '-r', action='store_true',
                        dest='selectivityrandom',
                        help='Estimate selectivity by randomly choosing the fraction of frames. Not setting will do fixed rate.')
    parser.add_argument('--pruneplans', '-p', action='store_true',
                        dest='pruneplans',
                        help='Enable plan pruning for certain models')
    parser.add_argument('--query', '-q', type=str,
                        required=False, default='angrybernie',
                        choices=['angrybernie', 'dunk', 'amsterdamdock', 'deepface'],
                        dest='query',
                        help='Query to run (Default: angrybernie)')
    parser.add_argument('--canary', '-c', type=str, required=True, dest='canary', help='canary input video')
    parser.add_argument('-n', required=True, type=str, dest='out_name', help="output name")

    return parser.parse_args()


def main(args):
    query = args.query
    canary = args.canary
    sel_fraction = args.selectivityfraction
    sel_random = args.selectivityrandom
    prune_plans = args.pruneplans
    do_cache = True

    viva = VIVA(caching=do_cache)
    cp = None
    p = None
    f1_threshold = 0.8
    if query == 'angrybernie':
        from viva.plans.angry_bernie_plan import AngryBernieCanaryPlan as cp
        from viva.plans.angry_bernie_plan import AngryBerniePlan as p
    elif query == 'amsterdamdock':
        from viva.plans.amsterdam_dock_plan import AmsterdamCanaryPlan as cp
        from viva.plans.amsterdam_dock_plan import AmsterdamDockPlan as p
    elif query == 'dunk':
        from viva.plans.dunk_plan import DunkCanaryPlan as cp
        from viva.plans.dunk_plan import DunkPlan as p
    elif query == 'deepface':
        from viva.plans.deepface_plan import DeepFaceCanaryPlan as cp
        from viva.plans.deepface_plan import DeepFacePlan as p
    elif cp is None and p is None:
        print('%s is not an implemented query' % query)
        return

    all_plans = p.all_plans

    keys = {}
    keys['f1'] = {'params': 'canary:angrybernie_with_jake.mp4_chunk_size_s:5_end_fraction:1_fps:1_height:240_inputs:7465166314c2b3a139d432065467aa0b8123c2e894396ddcf0170889_query:angrybernie_selectivityfraction:0.1_selectivityrandom:False_start_fraction:0_width:360',
                  'key': '5b4af47a3896e7546a5f5831ef4d8669f5b23f1944b2513da1557549'}
    # keys['f1'] = {'params': 'canary:dock.mp4_chunk_size_s:5_end_fraction:1_fps:1_height:240_inputs:4434c1a77c8cf87bcafb727a389f73920e798ccff8077b5e0918a380_query:amsterdamdock_selectivityfraction:0.1_selectivityrandom:False_start_fraction:0_width:360',
    #               'key': '4805be52c1e868f2921fd8d99bb39608f433d30b6d398cca286e667f'}
    # keys['f1'] = {'params': 'canary:turn_left.mp4_chunk_size_s:5_end_fraction:1_fps:1_height:240_inputs:4434c1a77c8cf87bcafb727a389f73920e798ccff8077b5e0918a380_query:amsterdamdock_selectivityfraction:0.1_selectivityrandom:False_start_fraction:0_width:360',
    #               'key': 'dba14873ba8bda37cb0b957fc09f696437e59a93d700e446d13a7040'}
    # keys['f1'] = {'params': 'canary:color_woman.mp4_chunk_size_s:5_end_fraction:1_fps:1_height:240_inputs:283a54b4d87b173e16c4e74534fa53ba4fe302fba6803119085d1f34_query:deepface_selectivityfraction:0.1_selectivityrandom:False_start_fraction:0_width:360',
    #                   'key': 'd8545daa18470e95465b2ec9252e1b4d6cfc64d7c5a542818e6bc8b6'}

    canary_name = pathlib.Path(canary).stem

    fids = []
    df_c = []
    df_i = []
    log_times = {}

    opt = Optimizer(
        all_plans, df_i, fids, viva, sel_fraction, sel_random,
        f1_threshold=f1_threshold, keys=keys, prune_plans=prune_plans
    )
    calculate_f1_scores(opt, args.out_name)

    print(f'Done profiling: {query}.')


if __name__ == '__main__':
    """
    Given a canary, produce its f1 score and selectivity for each plan
    and write them to resource/
    """
    main(get_args())
