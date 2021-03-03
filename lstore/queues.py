from collections import deque
from lstore.transaction import Transaction
from lstore.config import *

class Batch:
    """
    Holds Transactions ready for consumption by PlanningWorkers
    Queue fills as Xacts come in and get then get moved to a batch until it fills
    then batch gets flagged for processing
    """

    def __init__(self):
        self.xact_queue = deque()
        self.xact_batch = []
        pass

    def batch_xact(self):
        """
        fills a batch with transactions from the xact_queue
        """
        """
        TODO config would determine batch size, may want to align group queue count
        
        If the batch is full we would need to let the PlanningWorkers know the Batch is ready for consumption
        also need to determine how to handle partial batches for consumption
        
        """
        if len(self.xact_batch) < BATCH_SIZE:
            xact = self.xact_queue.popleft()
            self.xact_batch.append(xact)


    def queue_xact(self, transaction: Transaction):
        self.xact_queue.append(transaction)


class PriorityGroups:
    """
    Holds low and high priority Groups that each contain a set of transaction queues
    """

    def __init__(self):
        self.high_priority = Group()
        self.low_priority = Group()


class Group:
    """
    Holds queues of Transactions
    """
    def __init__(self):
        # TODO need to determine amount of Queues, self.queues will be a list of deque
        self.queues = []
