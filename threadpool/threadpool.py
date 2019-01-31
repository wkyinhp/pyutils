from threading import Thread, Lock
import multiprocessing
from queue import Queue, PriorityQueue, Empty
import logging
import time
import itertools
import numpy as np

class Task(object):
    '''
    Task is a class that wrap the func to be run
    '''
    def __init__(self, func, *, args=None, kwargs=None, clean_up=False):
        '''
        Input:
          func (function): the function to be run
          args (list): positional args to be passed to func
          kwargs (dict): keyword args to be passed to func
          clean_up (bool): if the task manager is closed, only those tasks with
              this flag is True can be run. These task should be those responsible
              for cleaning up, and they should NOT be looping
        '''
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        self.clean_up = clean_up
        
    def run(self):
        '''simply run the task'''
        self.func(*self.args, **self.kwargs)
        
class TaskSchedule(object):
    '''
    A class that contains to task to be run and the scheduled time of running
    '''
    def __init__(self, task: Task, schedule_time: float):
        ''' '''
        self.schedule_time = schedule_time
        self.task = task
        
    def __lt__(self, other):
        return self.schedule_time < other.schedule_time
    
    def __gt__(self, other):
        return self.schedule_time > other.schedule_time
    
def task_schedule(func, schedule_time, args=None, kwargs=None, clean_up=False):
    '''TaskSchedule helper'''
    task = Task(func, args=args, kwargs=kwargs, clean_up=clean_up)
    return TaskSchedule(task, schedule_time=schedule_time)

class ProgramExitedError(Exception):
    '''Exception raised after the task manager is closed (program exited)'''
    pass
        
class ProgramExit(Exception):
    '''Exception raised for killing a specific helper thread of the task manager
       for:
        1) reduce the number of helper threads
        2) actually quit the program
    '''
    pass

class LoopTerm(Exception):
    '''Raised to terminate the repeating of the Task '''
    pass

class _PoolCloseHandler(object):
    '''Helper class returned if the threadpool is closed with context manager
       
       Usage:
       with pool.end():
           do some cleanup
    '''
    def __init__(self, pool):
        self.pool = pool
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_tb):
        self.pool.finalize()
        
class ThreadPool(object):
    '''Holds a list of helper threads
       
       Attributes:
       n_threads (int): number of helper threads
       queue (Queue): a queue of Task which are scheduled to run now
       
       Methods:
       end(self)
       put(task, *, block=True, timeout=None)
       set_n_threads(self, n_threads)
       add_threads(self, n)
       remove_threads(self, n)
       clear(self)
    '''
    def __init__(self, *, n_threads=None, maxsize=0):
        '''
        Input:
          n_threads (int): Number of helper threads,
              if not provided, the cpu count of the pc is used
          maxsize (int): the maximum of the Task queue,
              if not provided, there is not limit to the size
        '''
        self._lock = Lock()
        n_threads = n_threads or multiprocessing.cpu_count()
        self.queue = Queue(maxsize)
        self._n_threads = 0
        self.i = itertools.count()
        for i in range(n_threads):
            self._construct_thread()
        self._closed = False
        
    def put(self, task, *, block=True, timeout=None):
        '''Put a Task or a list of Task into pool.Queue
        '''
        tasks = task if isinstance(task, list) else [task]
        for t in tasks:
            if self._closed and not t.clean_up:
                logging.error('Thread Pool is closed, cannot add new Tasks to it')
                continue
            self.queue.put(t, block=block, timeout=timeout)
        
    def _construct_thread(self, name=None):
        '''Construct a new helper thread and kick it start
        '''
        name = name or next(self.i)
        thread = Thread(target=self._run, name='Python Worker {}'.format(name))
        thread.start()
        with self._lock:
            self._n_threads += 1
        
    def _destroy_thread(self):
        '''
        This does not guarantee a successful destruction of thread
        '''
        self.queue.put(Task(self._exit_task, clean_up=True))
        
    def _exit_task(self):
        '''Function to be run by thread to kill it'''
        with self._lock:
            self._n_threads -= 1
        #the thread is killed after running this line
        raise ProgramExit
        
    def _run(self):
        '''the looping function of the threads
           In the runs, only ProgramExit exception will end up in termination
        '''
        while True:
            try:
                task = self.queue.get(block=True, timeout=None)
                task.run()
            except ProgramExit:
                return #quit the program, so quit the thread
            except:
                pass
                
    def set_n_threads(self, n_threads):
        '''Change the num of helper threads
           If n_threads > self.n_threads, new threads will be constructed
           Else, threads will be destructed but the exact number may not be guaranteed
        '''
        max_threads = 32
        if n_threads > max_threads:
            logging.warning('Num of threads of ThreadPool to be set > {}, capped at {}'.format(
                max_threads, max_threads))
        n_threads = min(max(n_threads, 0), 32)
        diff = n_threads - self.n_threads
        if diff > 0:
            for i in range(diff):
                self._construct_thread(name=self._n_threads)
        else:
            for i in range(-diff):
                self._destroy_thread()
                
    def add_threads(self, n):
        self.set_n_threads(self.n_threads + n)
        
    def remove_threads(self, n):
        self.set_n_threads(self.n_threads - n)
        
    def clear(self):
        '''Clear self.queue (throw away all the tasks in the queue)
        '''
        while True:
            try:
                self.queue.get(False)
            except Empty:
                break
                
    def end(self):
        '''
        usage:
        
        with thread_pool.end():
            some clean_up
        '''
        #clear the queue
        self._closed = True
        self.clear()
        return _PoolCloseHandler(self)
        
    def finalize(self):
        for i in range(self.n_threads):
            self._destroy_thread()
    
    @property
    def n_threads(self):
        return self._n_threads
    
class TaskScheduler(object):
    '''Store a PriorityQueue of TaskSchedule objects
    
       Attributes:
         schedule (PriorityQueue): the queue for the TaskSchedule
       
       Methods:
         put(self, func, schedule_time, *, args=None, kwargs=None, block=True, timeout=None,
             clean_up=False)
         put_task(self, task, schedule_time, *, block=True, timeout=None)
         repeat(self, func, schedule_time=None, *, args=None, kwargs=None, block=True,
                timeout=None, interval=None, exception=LoopTerm)
         get(self, cutoff=None)
    '''
    def __init__(self, maxsize=0):
        '''
        Input:
          maxsize (int): max size of the priority queue
        '''
        self.schedule = PriorityQueue(maxsize)
        
    def put(self, func, schedule_time, *, args=None, kwargs=None, block=True, timeout=None,
            clean_up=False):
        '''put the task with specific schedule_time into self.schedule'''
        self.schedule.put(task_schedule(func, schedule_time, args, kwargs, clean_up), block,
                          timeout)
        
    def put_task(self, task, schedule_time, *, block=True, timeout=None):
        '''put the task with specific schedule_time into self.schedule'''
        self.schedule.put(TaskSchedule(task, schedule_time), block=block, timeout=timeout)
        
    def repeat(self, func, schedule_time=None, *, args=None, kwargs=None, block=True,
               timeout=None, interval=None, exception=LoopTerm):
        '''
        Inputs:
          func: the function to be repeated
          schedule_time: the schedule time of the first run of the repeated function
          args: 
          kwargs: 
          block: 
          timeout: 
          interval: time between two consecutive repeating run (in secs)
          exception: Exception subclasses or a list of Exceptions, these are expected to be 
              raised in case the repeated function has to be ended
        '''
        if interval is None:
            raise ValueError('Cannot loop a function without interval')
        func = self._repeat(func, args, kwargs, interval, block, timeout, exception)
        self.schedule.put(task_schedule(func, schedule_time, args=args, kwargs=kwargs),
                          block=block, timeout=timeout)
        
    def _repeat(self, func, args, kwargs, interval=None, block=True, timeout=None,
                exception=LoopTerm):
        def _wrapper(*_args, **_kwargs):
            try:
                func(*_args, **_kwargs)
            except exception:
                return
            scheduled_time = time.time()+interval
            self.schedule.put(task_schedule(_wrapper, scheduled_time, args=args, kwargs=kwargs),
                              block=block, timeout=timeout)
        return _wrapper
        
    def get(self, cutoff=None):
        '''get all the task before the cutoff
           Input:
             cutoff (float): the timestamp of cutoff, default is time.time()
             
           Return:
             tasks: a list of Task object
        '''
        cutoff = cutoff or time.time()
        tasks = []
        #used by only one thread, should be OK
        while not self.schedule.empty() and self.schedule.queue[0].schedule_time <= cutoff:
            schedule = self.schedule.get(block=False)
            tasks.append(schedule.task)
        return tasks
    
class TaskManager(TaskScheduler, ThreadPool):
    '''TaskManager constantly manages schedule and task queue
    
       Attributes:
         n_threads
         schedule
         queue
         
       Methods:
         start(self, timeout=0.1)
         end(self)
         put(self, func, schedule_time, *, args=None, kwargs=None, block=True, timeout=None,
             clean_up=False)
         put_task(self, task, schedule_time, *, block=True, timeout=None)
         repeat(self, func, schedule_time=None, *, args=None, kwargs=None, block=True,
                timeout=None, interval=None, exception=LoopTerm)
         set_n_threads(self, n_threads)
         add_threads(self, n)
         remove_threads(self, n)
         clear(self)
         active(self)
         has_tasks_scheduled(self)
         has_tasks_pending(self)
    '''
    def __init__(self, n_threads=None, tp_maxsize=0, ts_maxsize=0):
        '''
        Input:
          n_threads: num of working threads
          tp_maxsize: the max size of the underlying Queue of ThreadPool
          ts_maxsize: the max size of the underlying PriorityQueue of TaskScheduler
        '''
        TaskScheduler.__init__(self, maxsize=ts_maxsize)
        ThreadPool.__init__(self, n_threads=n_threads, maxsize=tp_maxsize)

        
    def start(self, timeout=0.1):
        '''
        Input:
          timeout (float): time interval to check tasks in schedules (in secs)
        '''
        thread = Thread(target=self._manage, kwargs={'timeout': timeout})
        thread.start()
        
    def end(self):
        '''
        Usage:
        with task_manager.end():
            do some clean up
        '''
        return ThreadPool.end(self)
        
    def _manage(self, timeout=1):
        while self.active():
            tasks = TaskScheduler.get(self)
            ThreadPool.put(self, tasks)
            time.sleep(timeout)
    
    def active(self):
        return not (self._closed and self.n_threads == 0)
    
    def has_tasks_scheduled(self):
        '''
        return True if some tasks are scheduled to be run in some future time, False otherwise
        '''
        return not self.schedule.empty()
    
    def has_tasks_pending(self):
        '''
        return True if some tasks are eligible to be run at the moment but not yet allocated to
        any worker, False otherwise
        '''
        return not self.queue.empty()
    
if __name__ == '__main__':
    def repeat_test_func():
        if np.random.randint(0, 10) == 0:
            raise LoopTerm
        print('repeat_test_func: {}'.format(time.time()))
    
    tm = TaskManager(n_threads=3, tp_maxsize=200, ts_maxsize=200)
    tm.start(0.5)
    lock = Lock()
    
    assert tm.n_threads == 3
    
    tm.set_n_threads(4)
    time.sleep(1)
    assert tm.n_threads == 4
    
    tm.set_n_threads(2)
    time.sleep(1)
    assert tm.n_threads == 2
    
    def test(scheduled_time):
        time.sleep(0.1)
        with lock:
            print('Scheduled_time: {}, Current_time: {}'.format(scheduled_time, time.time()))
        
    for i in range(5):
        scheduled_time = time.time() + np.random.randint(0, 3)
        tm.put(test, scheduled_time, args=[scheduled_time])
        
    for i in range(5):
        scheduled_time = time.time() + np.random.randint(0, 3)
        task = Task(test, args=[scheduled_time])
        tm.put_task(task, scheduled_time)
        
    time.sleep(5)
    
    tm.repeat(repeat_test_func, schedule_time=time.time(), interval=0.5)
    time.sleep(15)
    
    with tm.end():
        pass