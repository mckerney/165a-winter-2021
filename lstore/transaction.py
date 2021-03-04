from lstore.query import *

from datetime import datetime

class Transaction:
    """
    Holds a Query and its arguments as well as relevant data for QueCC planning
    """
    def __init__(self):
        self.query_name = None
        self.query_func = None
        self.rid = None
        self.timestamp = datetime.now()
        self.key = None
        self.column = None
        self.columns = None
        self.start_range = None
        self.end_range = None


    def add_query(self, query_name: str, func, rid: int=None, *args):
        """
        Adds query methods and arguments to the transaction
        param query_name:   Name of query function
        param func:         Query function
        param rid:          RID for record being queried if there is one
        param *args:        Query arguments
        """

        self.query_name = query_name
        self.query_func = func

        if query_name == 'insert':
            self.columns = args[0]      # columns

        if query_name == 'delete':
            self.rid = rid
            self.key = args[0]          # key

        if query_name == 'select':
            self.rid = rid
            self.key = args[0]          # key
            self.column = args[1]       # column
            self.columns = args[2]      # query_columns

        if query_name == 'update':
            self.rid = rid
            self.key = args[0]          # key
            self.columns = args[1]      # columns

        if query_name == 'sum':
            # TODO: consult key index for rid and initialize self.rid for all involved? Might want to wrap each read?
            self.start_range = args[0]  # start_range
            self.end_range = args[1]    # end_range
            self.column = args[2]       # aggregate_column_index

        if query_name == 'increment':
            pass

    def run(self):
        return self.commit()

    def commit(self):
        """
        Run the Query
        """
        if self.query_name == 'insert':
            return self.query_func(self.columns)

        if self.query_name == 'delete':
            return self.query_func(self.key)

        if self.query_name == 'select':
            return self.query_func(self.key, self.column, self.columns)

        if self.query_name == 'update':
            return self.query_func(self.key, self.columns)

        if self.query_name == 'sum':
            return self.query_func(self.start_range, self.end_range, self.column)

        if self.query_name == 'increment':
            return
