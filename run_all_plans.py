import argparse
import logging
import os.path

import pandas as pd

from viva.utils.config import viva_setup, ConfigManager

config = ConfigManager()
# viva must be set here for pyspark utility
spake = viva_setup()


from viva.core.session import VIVA
from viva.core.utils import ingest, save_to_db


query_plan_map = {
    'angrybernie': 'angry_bernie',
    'amsterdamdock': 'amsterdam_dock',
    'dunk': 'dunk',
    'deepface': 'deep_face'
}


def query2plan(query: str):
    from importlib import import_module
    if query not in query_plan_map:
        raise NotImplementedError(f"The query [{query}] is unsupported")
    plan_module_name = query_plan_map[query] + '_plan'
    plan_name = plan_module_name.title().replace('_', '')
    module = import_module(f'viva.plans.{plan_module_name}')
    return getattr(module, plan_name)


parser = argparse.ArgumentParser()
parser.add_argument('--query', '-q', type=str,
                    required=False, default='angrybernie',
                    choices=['angrybernie', 'dunk', 'amsterdamdock', 'deepface'],
                    dest='query',
                    help='Query to run (Default: angrybernie)')
args = parser.parse_args()
log_times = {'total': 0.0}
do_cache = False
viva_session = VIVA(log_times, do_cache)
plans = query2plan(args.query)
plans_raw = plans.all_plans

result = []
file_path = config.get_value('logging', 'output')
file_name = os.path.join(file_path, args.query, f'all_plans_cost_{len(plans_raw)}')
if os.path.exists(file_name):
    saved_cost = pd.read_csv(file_name)
    saved_cost = saved_cost['plan'].to_list()
else:
    saved_cost = []

df_i = ingest(config.get_value('storage', 'input'))
df_i.cache()
df_i.count()
viva_session.run(df_i, plans_raw[69], plans.hints)

for p in plans_raw:
    plan_str = ','.join([node.out_column for node in p])
    if plan_str not in saved_cost:
        viva_session.reset_cache()
        log_times['total'] = 0.0
        viva_session.run(df_i, p, plans.hints)
        logging.warning(f'Finish: {plan_str}, time: {log_times["total"]}')
        df = pd.DataFrame({'plan': [plan_str], 'cost': [log_times['total']]})
        save_to_db(df, file_name)
