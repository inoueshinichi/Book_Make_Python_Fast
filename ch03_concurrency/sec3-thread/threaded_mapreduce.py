from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor as Executor
from time import sleep

from pprint import pprint

# report_progressには, 完了したジョブに関する統計情報を使って
# 0.5秒おきに呼び出されるコールバック関数が必要
def report_progress(futures, tag, callback):
    not_done = 1
    done = 0
    while not_done > 0:
        not_done = 0
        done = 0
        for fut in futures:
            if fut.done():
                done += 1
            else:
                not_done += 1
            
        sleep(0.5)
        if not_done > 0 and callback:
            callback(tag, done, not_done)

def async_map(executor, mapper, data):
    futures = []
    for datum in data:
        futures.append(executor.submit(mapper, datum))
    return futures

def map_less_naive(executor, my_input, mapper):
    map_results = async_map(executor, mapper, my_input)
    return map_results

def map_reduce_less_naive(my_input, mapper, reducer, callback=None):
    with Executor(max_workers=2) as executor:
        futures = async_map(executor, mapper, my_input)
        # すべてのMapタスクの進捗を報告
        report_progress(futures, 'map', callback)
        # 結果は実際はfutureであるため, futureオブジェクトから結果を取得
        map_results = map(lambda f: f.result(), futures)
        distributor = defaultdict(list)
        for key, value in map_results:
            distributor[key].append(value)
        futures = async_map(executor, reducer, distributor.items())
        # すべてのReduceタスクの進捗を報告
        report_progress(futures, 'reduce', callback)
        # 結果は実際はfutureであるため, futureオブジェクトから結果を取得
        results = map(lambda f: f.result(), futures)
    return results

def emitter(word):
    return word, 1

counter = lambda emitted: (emitted[0], sum(emitted[1]))

def reporter(tag, done, not_done):
    print(f"Operation {tag}: {done}/{done + not_done}")


def case_1():
    words = 'Python is great Python rock'.split(' ')
    with Executor(max_workers=4) as executor:
        maps = map_less_naive(executor, words, emitter)
        pprint(maps)
        print(maps[-1])
        not_done = 1
        while not_done > 0:
            not_done = 0
            for fut in maps:
                not_done += 1 if not fut.done() else 0
                sleep(1)
                print(f"Still not finalized: {not_done}")

def case_2():
    words = 'Python is great Python rock'.split(' ')
    a = map_reduce_less_naive(words, emitter, counter, reporter)
    for i in sorted(a, key=lambda x: x[1]):
        print(i)

if __name__ == "__main__":
    # case_1()
    case_2()