from lstore.table import *
from lstore.record import *
from lstore.index import Index

class TransactionWorker:
    """
    Creates a transaction worker object.
    """
    def __init__(self):
        self.stats = {}
        self.transaction = None
        self.result = 0
        pass


class PlanningWorker(TransactionWorker):
    """
    PlanningWorkers are responsible for moving transactions to the PriorityGroups
    """
    # TODO needs to pull transactions from the Batch and enqueue them in the Priority Groups

    def enqueue_priority_group(self):
        pass

    def activate_worker(self):
        run = True
        while run:
            # checking
            # maki
            #
            #
            # if batcher.batch_ready
            #     consume batch
            #
            # sleep



class ExecutionWorker(TransactionWorker):
    """
    ExecutionWorkers are responsible for executing transactions in the PriorityGroups
    """
    # TODO needs to pull transactions from the Priority Groups and execute them

    def run(self):
        """
        Runs a transaction
        """
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
