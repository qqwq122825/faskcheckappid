"""Synthetic, network-disabled queue timing lab; independent of the login runtime."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import multiprocessing as mp
from pathlib import Path
import queue
import statistics
import time


def _worker(incoming,outgoing,threads,resource,work_seconds):
    import socket
    def deny(*args,**kwargs):raise RuntimeError('Local queue lab forbids network')
    socket.socket.connect=deny;socket.socket.connect_ex=deny
    socket.create_connection=deny;socket.getaddrinfo=deny
    import threading
    cpu_started=time.process_time()
    def execute(number,submitted):
        started=time.perf_counter_ns()
        if resource is not None:resource.acquire()
        acquired=time.perf_counter_ns()
        try:time.sleep(work_seconds)
        finally:
            finished=time.perf_counter_ns()
            if resource is not None:resource.release()
        outgoing.put(('result',number,submitted,started,acquired,finished,threading.get_ident()))
    with ThreadPoolExecutor(max_workers=threads) as executor:
        while True:
            item=incoming.get()
            if item is None:break
            executor.submit(execute,*item)
    outgoing.put(('cpu',time.process_time()-cpu_started))


def distribution(values):
    values=sorted(values)
    return {'mean_ms':round(statistics.mean(values),3),
            'p50_ms':round(statistics.median(values),3),
            'p95_ms':round(values[max(0,math.ceil(len(values)*.95)-1)],3)}


def benchmark(*,tasks=1200,processes=10,threads=100,work_ms=50,resource_slots=0):
    if any(type(v) is not int or v<1 for v in (tasks,processes,threads)):
        raise ValueError('tasks/processes/threads 必须为正整数')
    if not math.isfinite(work_ms) or work_ms<0 or type(resource_slots) is not int or resource_slots<0:
        raise ValueError('work_ms/resource_slots 不能为负')
    context=mp.get_context('spawn');outgoing=context.Queue()
    incoming=[context.Queue() for _ in range(processes)]
    resource=context.Semaphore(resource_slots) if resource_slots else None
    workers=[context.Process(target=_worker,args=(channel,outgoing,threads,resource,work_ms/1000),
                             name='local-queue-lab') for channel in incoming]
    stages={key:[] for key in ('submit_to_start','resource_wait','work','return_to_collector','end_to_end')}
    ids=set();thread_ids=set();cpu=[];main_cpu=time.process_time();started=time.perf_counter()
    try:
        for worker in workers:worker.start()
        spawn_seconds=time.perf_counter()-started
        for number in range(tasks):incoming[number%processes].put((number,time.perf_counter_ns()))
        for channel in incoming:channel.put(None)
        while len(ids)<tasks or len(cpu)<processes:
            try:message=outgoing.get(timeout=15)
            except queue.Empty:raise RuntimeError('Worker failed or made no progress for 15 seconds') from None
            received=time.perf_counter_ns()
            if message[0]=='cpu':cpu.append(message[1]);continue
            _,number,submitted,begin,acquired,finished,thread_id=message
            if number in ids:raise RuntimeError('Duplicate result')
            ids.add(number);thread_ids.add((number%processes,thread_id))
            for key,value in zip(stages,(begin-submitted,acquired-begin,finished-acquired,
                                        received-finished,received-submitted)):
                stages[key].append(value/1_000_000)
        elapsed=time.perf_counter()-started
        return {'tasks':tasks,'processes':processes,'threads_per_process':threads,
                'configured_capacity':processes*threads,'participating_threads':len(thread_ids),
                'simulated_resource_slots':resource_slots or 'unlimited','simulated_work_ms':work_ms,
                'elapsed_seconds':round(elapsed,4),'spawn_calls_seconds':round(spawn_seconds,4),
                'tasks_per_second':round(tasks/elapsed,2),
                'cpu_seconds_measured_sections':round(sum(cpu)+time.process_time()-main_cpu,4),
                'cpu_scope':'main plus worker body; child interpreter import/startup is excluded',
                'stages':{key:distribution(values) for key,values in stages.items()},
                'duplicate_or_missing_results':0,'network_requests':0,
                'collector':'blocking get on one local result queue; no polling delay',
                'note':'Only synthetic sleeps. These measurements do not identify a real provider bottleneck. Median stage durations are not additive.'}
    finally:
        for worker in workers:
            if worker.pid is None:continue
            worker.join(2)
            if worker.is_alive():worker.terminate();worker.join(2)
            if worker.is_alive():worker.kill();worker.join(2)
            worker.close()
        for channel in incoming+[outgoing]:channel.cancel_join_thread();channel.close()


def main():
    parser=argparse.ArgumentParser(description='无网络的本地多进程/线程队列计时实验')
    parser.add_argument('--tasks',type=int,default=1200)
    parser.add_argument('--processes',type=int,default=10)
    parser.add_argument('--threads',type=int,nargs='+',default=[100,200])
    parser.add_argument('--work-ms',type=float,default=50)
    parser.add_argument('--resource-slots',type=int,default=0,
                        help='显式模拟共享资源容量，0 不限制；不影响正式程序')
    parser.add_argument('--output',default='data/validation/local-queue-lab.json')
    args=parser.parse_args()
    runs=[]
    for threads in args.threads:
        result=benchmark(tasks=args.tasks,processes=args.processes,threads=threads,
                         work_ms=args.work_ms,resource_slots=args.resource_slots)
        runs.append(result);print(json.dumps(result,ensure_ascii=False),flush=True)
    path=Path(args.output);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps({'scope':'isolated local synthetic queue lab','runs':runs},ensure_ascii=False,indent=2)+'\n')


if __name__=='__main__':
    mp.freeze_support();main()
