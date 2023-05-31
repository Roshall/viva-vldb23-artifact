import json
import os
import pickle
import sys
from os import path
from time import perf_counter

import numpy as np
import torch
from sklearn.linear_model import LinearRegression

from viva.core.utils import gen_input_df

basepath = path.dirname(__file__)
sys.path.append(path.abspath(path.join(basepath, '../../')))

from viva.utils.config import viva_setup, ConfigManager

spark = viva_setup()
config = ConfigManager()

use_cuda = torch.cuda.is_available() and config.get_value('execution', 'gpu')
device = torch.device('cuda' if use_cuda else 'cpu')

from viva.plans.transfer_plan import TransferPlan

# frames = [0, 1, 500, 1000, 2000, 3000]
frames = [5000]
iterations = 5
t_sleep = 0
h = config.get_value('ingest', 'height')
w = config.get_value('ingest', 'width')
fname = 'data/gpu_data_transfer.json'


def warmup(plan):
    df_i = gen_input_df(spark, config.get_value('storage', 'input'))
    for node in plan:
        df_i = node.apply_op(df_i)
    print('num frames', df_i.count())
    size = (1, 240, 360, 3)
    tensor = torch.rand(size, dtype=torch.float32)
    tensor = tensor.to(device)
    print('warm up done')


def execute_transfer(it=2, latencies={}):
    plan = TransferPlan.all_plans[0]
    warmup(plan)

    for f in frames:
        if str(f) in latencies:
            print(f'skipping {f}')
            continue

        latencies[f] = []

        for i in range(0, it):
            df_i = gen_input_df(spark, config.get_value("storage", "input"))
            df = df_i.limit(f)
            for node in plan:
                name = node.out_column
                s = perf_counter()
                df = node.apply_op(df)
                _ = df.count()
                t = perf_counter()
                d = t - s
                if name == 'transfer':
                    latencies[f].append(d)

        avg_lat = sum(latencies[f]) / len(latencies[f])
        latencies[f] = avg_lat
        if avg_lat < 1:
            print('{0}: {1:.2f}ms'.format(f, avg_lat * 1e3))
        else:
            print('{0}: {1:.2f}s'.format(f, avg_lat))

    return latencies


def fit_and_save(data):
    x_train = np.array([k for k in data]).reshape((-1, 1))
    y_train = np.array([data[k] for k in data])
    model = LinearRegression().fit(x_train, y_train)
    fname = 'data/gpu_data_transfer_model.pkl'
    pickle.dump(model, open(fname, 'wb'))

    return model


if __name__ == '__main__':
    if os.path.exists(fname):
        with open(fname, 'r') as fd:
            loaded_latencies = json.load(fd)
    else:
        loaded_latencies = {}

    latencies = execute_transfer(iterations, loaded_latencies)
    with open(fname, 'w') as fd:
        json.dump(latencies, fd, indent=4)

    model = fit_and_save(latencies)

    x_pred = np.array([500, 5000]).reshape((-1, 1))
    y_pred = model.predict(x_pred)
    print(f'Predicted latencies for {x_pred}:', y_pred)
