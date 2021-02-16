from lstore.table import Table
from lstore.bufferpool import *
import os
import shutil
import json

class Database():

    def __init__(self):
        # store tables in a dictionary
        self.table_directory = {}
        self.tables = {}
        self.bufferpool = None
        self.root_name = None
        pass

    def open(self, path):
        '''
        Open takes in a path to the root of the file system
        '''
        # Check if root path already exists and set the root_name
        if os.path.isdir(path):
            self.root_name = path

        else: # Make a new root for this database
            os.mkdir(path)
            self.root_name = path

        self.bufferpool = Bufferpool(path)

        # TODO : Load in table information and create simplfied table objects
        # TODO : Read in indexes and page directories


    def close(self):
        '''
        Close checks all the dirty bits and writes updates back to disk; saves page_directories and indexes as json files
        '''
        # Save all the table data
        table_directory_as_json = json.dumps(self.table_directory)
        table_directory_file = open(f"{self.root_name}/table_directory.json", "w")
        table_directory_file.write(table_directory_as_json)
        table_directory_file.close()

        # go through every table and save the page directorys
        for table_info in self.table_directory.values():
            table_name = table_info.get("name")
            table = self.tables[table_name]
            did_close = table.close_table_page_directory()
            if not did_close:
                # TODO raise exception
                print(f"Could not close the page directory: {table_name}")
                return False
        
        # Write all dirty values back to disk
        self.bufferpool.commit_all_frames()

        # TODO : save indexes as json

        return True

    """
    # create_table makes a new directory inside root called name and adds it to our table directory; makes a new table object
    :param name: string         # Table name
    :param num_columns: int     # Number of Columns: all columns are integer
    :param key: int             # Index of table key in columns
    """
    def create_table(self, name: str, num_columns: int, key: int) -> Table:
 
        table_path_name = f"{self.root_name}/{name}"
        if os.path.isdir(table_path_name):
            # TODO let the user know this name is already taken
            print(f"Sorry the name {name} is already taken")
            return False
        else:
            os.mkdir(table_path_name)
        
        # TODO : simplify table object down to the bare minimum
        table = Table(name, num_columns, key, path=table_path_name, bufferpool=self.bufferpool)
        self.tables[name] = table

        # Add table information to table directory
        self.table_directory[name] = {
            "name": name,
            "table_path_name": table_path_name,
            "num_columns": num_columns,
            "key": key
        }
        return table


    def delete_directory(self, path):
        shutil.rmtree(path)


    def drop_table(self, name):
        '''
        drop_table removes the table from self.tables and self.tables_direcroty; deletes the table's directory in disk and its contents
        '''
        if name in self.table_directory:
            table_directory = self.table_directory[name]["table_path_name"]
            if os.path.isdir(table_directory):
                self.delete_directory(table_directory)
                del self.table_directory[name]
                del self.tables[name]
                return True
            else:
                return False
        else:
            return False

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.table_directory[name]
