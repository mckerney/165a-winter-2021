from lstore.table import *
from lstore.record import *
from lstore.index import Index

class Transaction:
    """
    Holds a Query and its arguments as well as relevant data for QueCC planning
    """
    def __init__(self):
        self.query = None
        self.rid = None
        self.timestamp = None
        pass


    def add_query(self, func, *args):
        """
        Adds query methods and arguments to the transaction
        param func:         Query function
        param *args:        Query arguments
        """

        # TODO: consult key index for rid and initialize self.rid
        # TODO: time stamp Xact
        self.query = (func, args)

    def run(self):
        return self.commit()

    def commit(self):
        """
        Run the Query
        """

        query_func = self.query[0]
        args = self.query[1]

        return query_func(args)
