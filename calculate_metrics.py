import argparse
import os
import pathlib
import random

from viva.utils.config import viva_setup, ConfigManager

spark = viva_setup()
config = ConfigManager()

from pprint import pprint

from viva.core.session import VIVA
from viva.core.optimizer import Optimizer
from viva.core.utils import (
    build_row, ingest, keygenerator, hash_input_dataset, create_log_dict,
    gen_canary_results
)


def canary_frame_ids(plan, session, df, logs, canary_name):
    plan_c = plan.all_plans[0]
    df_o = session.run(df, plan_c, {}, canary_name)
    return [r.id for r in df_o.select(df_o.id).collect()]


def listofdict2dictoflist(data: list[dict]):
    if not data:
        return None
    result = {'plan': [], 'total': []}
    for inner_dict in data:
        for key in result:
            result[key].append(inner_dict[key])
    return result


def calculate_f1_scores(opt, hints, df_c, log_times, canary_name):
    # calculate selectivity on input dataset using all plans with hints and get optimal plan
    # log_times['real_selectivity'] = opt.sel_profiles
    # update the input to the optimizer but don't reset.
    # Need to estimate plans using calculated selectivities
    # opt.set_df(df_c)
    # opt.set_hints(hints)
    # opt.set_canary_name(canary_name)

    # ignore returned best plan just need computation
    counter = 0
    for _ in range(200):
        random.shuffle(opt.plans)
        opt.get_optimal_plan()
        counter += opt.skip_candidate
        # print('='*20, "skip plans avg:", opt.skip_candidate)
        opt.skip_candidate = 0
        opt.reset_cache()
    print("="*20, "skip plans avg: ", counter/200)
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

    return parser.parse_args()


def main(args):
    query = args.query
    canary = args.canary
    sel_fraction = args.selectivityfraction
    sel_random = args.selectivityrandom
    prune_plans = args.pruneplans
    do_cache = False

    # load canary and dataset
    videos_path = config.get_value('storage', 'input')
    # df_c = ingest(custom_path=build_row(canary))
    # df_i = ingest(videos_path)

    # hash input dataset
    # input_dataset_hash = hash_input_dataset(df_i)
    keys = {}
    # log_times = create_log_dict(vars(args), config, input_dataset_hash)
    # keys['selectivity'] = keygenerator(log_times)
    # log_times['canary'] = os.path.basename(canary)
    # keys['f1'] = keygenerator(log_times)
    # keys['f1'] = {'params': 'canary:new_interview.mp4_chunk_size_s:5_end_fraction:1_fps:1_height:240_inputs:7465166314c2b3a139d432065467aa0b8123c2e894396ddcf0170889_query:angrybernie_selectivityfraction:0.1_selectivityrandom:False_start_fraction:0_width:360',
    #               'key': '11a49726a687da79915d2b8a616013276d5a7868f7177d48c1009b90'}
    keys['f1'] = {'params': 'canary:angrybernie_with_jake.mp4_chunk_size_s:5_end_fraction:1_fps:1_height:240_inputs:54f6dcc91dafe6ac9d48879bc3a1dc68f70d87e3200ced2d8ca9f5b6_query:angrybernie_selectivityfraction:0.1_selectivityrandom:False_start_fraction:0_width:360',
                  'key': '0efbbe606b572b039c9f1ac1feb49168ce3650014c492fd1a31c873a'}
    # # # keys['f1'] = {'params': 'canary:street.mp4_chunk_size_s:5_end_fraction:1_fps:1_height:240_inputs:42a6c078f83e907bd5abc29dbd6712ac7117acbdc18269e4cb8c84bf_query:amsterdamdock_selectivityfraction:0.1_selectivityrandom:False_start_fraction:0_width:360',
    # # #               'key': 'c74acdd504a57c44e4a6cb1f82f54e004ad84d9baa397e7fd4a0d07f'}
    # # keys['f1'] = {'params': 'canary:car_turn_left_with_man.mp4_chunk_size_s:5_end_fraction:1_fps:1_height:240_inputs:42a6c078f83e907bd5abc29dbd6712ac7117acbdc18269e4cb8c84bf_query:amsterdamdock_selectivityfraction:0.1_selectivityrandom:False_start_fraction:0_width:360',
    #               'key': '6aee44d0635c93354d3f28faa35a44e9732a9d2ff3244864040b4297'}

    viva = VIVA(caching=do_cache)
    cp = None
    p = None
    f1_threshold = 0.6
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

    canary_name = pathlib.Path(canary).stem

    # Generate canary results
    # gen_canary_results(df_c, canary_name, p.all_plans)

    # calculate accuracy on canary using canary plan
    # fids = canary_frame_ids(cp, viva, df_c, log_times, canary_name)
    fids = []
    df_c = []
    df_i = []
    log_times = {}


    all_log = []
    opt = Optimizer(
        p.all_plans, df_i, fids, viva, sel_fraction, sel_random,
        f1_threshold=f1_threshold, keys=keys, prune_plans=prune_plans, all_log=all_log
    )
    calculate_f1_scores(opt, p.hints, df_c, log_times, canary_name)
    viva.log_time = listofdict2dictoflist(all_log)

    # prefix = f'{query}_{canary_name}_pick_holes_f10.0'
    # viva.print_logs(query, "", "", prefix + '.csv')
    # opt.save_plans(query, prefix)
    #
    # pprint(log_times)

    print(f'Done profiling: {query}.')


if __name__ == '__main__':
    """
    Given a canary, produce its f1 score and selectivity for each plan
    and write them to resource/
    """
    main(get_args())
