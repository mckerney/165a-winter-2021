from collections import deque
from lstore.transaction import Transaction
from lstore.config import *

import time
import threading

class Batcher:
    """
    Holds Transactions ready for consumption by PlanningWorkers
    Queue fills as Xacts come in and get then get moved to a batch until it fills
    then batch gets flagged for processing
    """

    def __init__(self):
        self.xact_queue = deque()
        self.xact_batch = []
        self.batch_ready = False
        self.high_priority_group = Group()
        self.low_priority_group = Group()
        self.high_planner = PlanningWorker(self.high_priority_group, self)
        # self.low_planner = PlanningWorker(self.low_priority_group, self)
        self.executor = ExecutionWorker(self)
        self.execution_mutex = threading.Lock()

    def batch_xact(self):
        """
        fills a batch with transactions from the xact_queue
        """
        """
        TODO config would determine batch size, may want to align group queue count
        
        If the batch is full we would need to let the PlanningWorkers know the Batch is ready for consumption
        also need to determine how to handle partial batches for consumption. If queue is empty and batch isn't
        full then process the batch
        """
        print('BATCHING')
        if len(self.xact_batch) < BATCH_SIZE:
            xact = self.xact_queue.popleft()
            self.xact_batch.append(xact)

        if len(self.xact_batch) == BATCH_SIZE:
            self.batch_ready = True


    def queue_xact(self, transaction: Transaction):
        print('QUEUEING')
        self.xact_queue.append(transaction)


class Group:
    """
    Holds queues of Transactions
    """
    def __init__(self):
        # TODO need to determine amount of Queues, self.queues will be a list of deque
        self.queues = [deque() for i in range(PRIORITY_QUEUE_COUNT)]


class PlanningWorker:
    """
    PlanningWorkers are responsible for moving transactions to the PriorityGroups
    """

    # TODO needs to pull transactions from the Batch and enqueue them in the Priority Groups
    def __init__(self, priority_group: Group, batcher: Batcher):
        self.group = priority_group
        self.batcher = batcher
        self.planner_thread = threading.Thread(target=self.do_work)
        self.planner_thread.daemon = False
        self.planner_thread.start()

    def enqueue_priority_group(self):
        pass

    def do_work(self):
        print('WORK STARTING')
        run = True

        while run:
            if self.batcher.batch_ready:
                print(f'PLANNING: {self.batcher.xact_batch}')
                for xact in self.batcher.xact_batch:
                    print(f'IN PLANNING WORKER DO WORK: {xact.query_name}')
                    xact_pop = self.batcher.xact_batch.pop(0)
                    print(f'IN PLANNING WORKER xact_pop: {xact_pop.query_name}')
                    self.group.queues[0].append(xact_pop)

            if len(self.batcher.xact_batch) == 0:
                self.batcher.batch_ready = False

            # time.sleep(0.5)


class ExecutionWorker:
    """
    ExecutionWorkers are responsible for executing transactions in the PriorityGroups
    """
    def __init__(self, batcher: Batcher):
        self.batcher = batcher
        #self.transaction = None
        self.exec_thread = threading.Thread(target=self.do_execution)
        self.exec_thread.daemon = False
        self.exec_thread.start()

    def do_execution(self):
        print('EXECUTE STARTING')
        run = True
        run_count = 0
        while run:

            if len(self.batcher.high_priority_group.queues[0]) > 0:
                print('EXECUTING')
                print('LOCKING')
                self.batcher.execution_mutex.acquire()
                transaction = self.batcher.high_priority_group.queues[0].popleft()
                print(f'RUNNING {transaction.query_name}')
                ret = transaction.run()
                print(f'ret: {ret}')
                print('RELEASE')
                self.batcher.execution_mutex.release()
