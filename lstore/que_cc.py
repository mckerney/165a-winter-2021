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
        self.xact_count = 0
        self.xact_meta_data = {
            # xact_id : [len of queries list, [], False]
        }
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


    def enqueue_xact(self, transaction: Transaction):
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

    def do_work(self):
        print('WORK STARTING')
        run = True

        while run:

            if self.batcher.batch_ready:
                print(f'PLANNING: {self.batcher.xact_batch}')

                # TODO select transaction by it's age, oldest first

                # TODO NICK Sort transactions based on timestamp
                for xact in self.batcher.xact_batch:

                    xact.id = self.batcher.xact_count
                    self.batcher.xact_meta_data[xact.id] = [len(xact.queries), [], False]
                    self.batcher.xact_count += 1
                    xact_pop = self.batcher.xact_batch.pop(0)

                    # TODO NICK Sort query list based on timestamp
                    for query in xact_pop.queries:
                        print(f'IN PLANNING WORKER DO WORK: {query.query_name}')
                        query.set_xact_id(xact.id)
                        index = query.key % PRIORITY_QUEUE_COUNT
                        self.group.queues[index].append(query)

            if len(self.batcher.xact_batch) == 0:
                self.batcher.batch_ready = False

            if self.is_done_running():
                run = False

    def is_done_running(self):
        is_done = False
        if len(self.batcher.xact_queue) == 0:
            is_done = True

        return is_done


class ExecutionWorker:
    """
    ExecutionWorkers are responsible for executing transactions in the PriorityGroups
    """
    def __init__(self, batcher: Batcher):
        self.batcher = batcher
        self.exec_thread = threading.Thread(target=self.do_execution)
        self.exec_thread.daemon = False
        self.exec_thread.start()

    def do_execution(self):
        print('EXECUTE STARTING')
        run = True
        while run:

            if len(self.batcher.high_priority_group.queues[0]) > 0:
                print('EXECUTING')
                print('LOCKING')

                q_op = self.batcher.high_priority_group.queues[0].popleft()
                print(f'RUNNING {q_op.query_name}')
                ret = q_op.run()
                self.batcher.xact_meta_data[q_op.xact_id][1].append(ret)

                # check if Xact is complete
                if self.batcher.xact_meta_data[q_op.xact_id][0] == len(self.batcher.xact_meta_data[q_op.xact_id][1]):
                    self.batcher.xact_meta_data[q_op.xact_id][2] = True

            if self.is_done_running():
                run = False

    def is_done_running(self):
        is_done = True
        for transaction in self.batcher.xact_meta_data:
            if not self.batcher.xact_meta_data[transaction][2]:
                is_done = False

        return is_done
