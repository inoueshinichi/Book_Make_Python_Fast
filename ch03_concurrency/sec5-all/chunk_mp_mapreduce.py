from collections import defaultdict
import marshal
import multiprocessing as mp
import sys
from time import sleep
import time
import types

def report_progress(map_returns, tag, callback):
    done = 0
    num_jobs = len(map_returns)
    while num_jobs > done:
        done = 0
        for ret in map_returns:
            if ret.ready():
                done += 1
        sleep(0.5)
        if callback:
            callback(tag, done, num_jobs - done)

def chunk0(my_list, chunk_size):
    for i in range(0, len(my_list), chunk_size):
        yield my_list[i:i + chunk_size]

def chunk(my_iter, chunk_size):
    chunk_list = []
    for elem in my_iter:
        chunk_list.append(elem)
        if len(chunk_list) == chunk_size:
            yield chunk_list
            chunk_list = []
    if len(chunk_list) > 0:
        yield chunk_list

def chunk_runner(fun_marshal, data):
    fun = types.FunctionType(marshal.loads(fun_marshal), globals(), 'fun')
    ret = []
    for datum in data:
        print(fun(datum))
        ret.append(fun(datum))
    return ret

def chunked_async_map(pool, mapper, data, chunk_size):
    async_returns = []
    for data_part in chunk(data, chunk_size):
        async_returns.append(pool.apply_async(
            chunk_runner, (marshal.dumps(mapper.__code__), data_part)
        ))
    return async_returns

def map_reduce(pool,
               my_input,
               mapper,
               reducer,
               chunk_size,
               callback=None):
    map_returns = chunked_async_map(pool, mapper, my_input, chunk_size)
    report_progress(map_returns, 'map', callback)
    map_results = []
    for ret in map_returns:
        map_results.extend(ret.get())
    distributor = defaultdict(list)
    for key, value in map_results:
        distributor[key].append(value)
    returns = chunked_async_map(pool, reducer, distributor.items(), chunk_size)
    report_progress(returns, 'reduce', callback)
    results = []
    for ret in returns:
        results.extend(ret.get())
    return results

def reporter(tag, done, not_done):
    print(f"Operation {tag}: {done}/{done + not_done}")

