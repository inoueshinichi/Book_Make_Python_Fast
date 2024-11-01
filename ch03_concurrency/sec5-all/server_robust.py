# 1. ユーザーの割り込みリクエスト(Ctrl + C)[SIGINT]を補足して、その時点でクリーンアップを開始する
# 2. 非同期サーバをキャンセルできる状態になったので, そのことも補足する必要がある
# 3. ワーカスレッドにクリーンアップが必要であることを通知する手段が必要になる

import asyncio
from functools import partial
import marshal
import multiprocessing as mp
import pickle
from queue import Empty, Queue
import signal # Ctrl + C
import threading
from time import sleep as sync_sleep
import types

import chunk_mp_mapreduce as mr

def handler_interrupt_signal(server):
    server.close()
    while server.is_serving():
        sync_sleep(0.1)

work_queue = Queue()
results_queue = Queue()
results = {} # KV

async def submit_job(job_id, reader, writer):
    writer.write(job_id.to_bytes(4, 'little'))
    writer.close()
    code_size = int.from_bytes(await reader.read(4), 'little')
    my_code = marshal.loads(await reader.read(code_size))
    data_size = int.from_bytes(await reader.read(4), 'little')
    data = pickle.loads(await reader.read(data_size))
    work_queue.put_nowait((job_id, my_code, data))

def get_results_queue():
    while results_queue.qsize() > 0:
        try:
            job_id, data = results_queue.get_nowait()
            results[job_id] = data
        except Empty:
            return

async def get_results(reader, writer):
    get_results_queue()
    job_id = int.from_bytes(await reader.read(4), 'little')
    data = pickle.dumps(None)
    if job_id in results:
        data = pickle.dumps(results[job_id])
        del results[job_id]
    writer.write(len(data).to_bytes(4, 'little'))
    writer.writer(data)

async def accept_requests(reader, writer, job_id=[0]):
    op = await reader.read(1)
    if op[0] == 0:
        await submit_job(job_id[0], reader, writer) # XXX Errors in async
    elif op[0] == 1:
        await get_results(reader, writer)

def worker(pool):
    while True:
        job_id, code, data = work_queue.get()
        if job_id == -1:
            break
        func = types.FunctionType(code, globals(), 'mapper_and_reducer')
        mapper, reducer = func()
        counts = mr.map_reduce(pool, data, mapper, reducer, 100, mr.reporter)
    print('Worker thread terminating')

# ProcessにCtrl + Cで勝手に強制終了しないように事前通達
def init_worker():
    # 割り込みシグナルを無視して, プールに伝搬されないようにする
    signal.signal(signal.SIGINT, signal.SIG_IGN)

async def main():
    server = await asyncio.start_server(accept_requests, '127.0.0.1', 1936)
    # マルチプロセッシングプールの初期化(つまり, プールは入力シグナルを無視する)
    mp_pool = mp.Pool(initializer=init_worker)
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, partial(handler_interrupt_signal, server=server))
    worker_thread = threading.Thread(target=partial(worker, pool=mp_pool))
    worker_thread.start()
    async with server:
        try:
            await server.serve_forever()

        # サーバ停止
        except asyncio.exceptions.CancelledError:
            print(f"Server cancelled")

    # -1を送信(ワーカによってサインアウトコマンドとして解釈される)
    work_queue.put((-1, -1, -1))
    worker_thread.join()
    mp_pool.close()
    mp_pool.join()
    print("Bye Bye!")

asyncio.run(main())



