# -*- coding: utf-8 -*-

"""
dbconn
============

Tornado singleton asynchronous DB connection pool module 
based on toradbapi (https://github.com/geerk/toradbapi) 

How to use the documentation
----------------------------
Documentation is available in one form: docstrings provided
with the code

The docstring examples assume that `dbconn` has been imported as `db`::

    >>> import db.dbconn as db

Code snippets are indicated by three greater-than signs::
  
    >>> x = 42
    >>> x = x + 1

Use the built-in ``help`` function to view a function's docstring::
    
    >>> help(db.initialize_db)
    ... # doctest: +SKIP


Copyright (c) 2016, Edgar A. Margffoy.
MIT, see LICENSE for more details.
"""

import os 
import sys
import toradbapi

# Sigleton object for DB connection access
db_pool = None

class DatabaseConnection(object):
    '''
    Simple Wrapper for toradbapi.toradbapi, simplifies
    and reduces the number of useful functions regarding
    applications that are constrained to execute transactions 
    '''
    def __init__(self, *args, **kwargs):
        '''
        Connection pool constructor, equivalent to 
        twisted.enterprise.adbapi.ConnectionPool
        
        Any positional or keyword arguments other than those documented here
        are passed to the DB-API object when connecting. Use these arguments to
        pass database names, usernames, passwords, etc.

        Parameters
        ----------
        dbapiName: an import string to use to obtain a DB-API compatible
            str - DB adaptor Python module, e.g., cx_oracle, psycopg2, etc
        cp_min: the minimum number of connections in pool
            int - Default Value: 3
        cp_max: int the maximum number of connections in pool
            int - Default Value: 5
        cp_noisy: Generate informational log messages during operation
            bool - Default Value: False

        Notes
        -----
        The complete documentation, along with a complete set of
        initialization parameters, can be found at:
        https://twistedmatrix.com/documents/current/api/twisted.enterprise.adbapi.ConnectionPool.html#__init__
        '''
        self._pool = toradbapi.ConnectionPool(*args, **kwargs)

    def run_transaction(self, *args, **kwargs):
        '''
        Start a transaction with the database and return the result.
        Equivalent to twisted.enterprise.adbapi.ConnectionPool.runInteraction

        The interaction is a callable object which will be executed in a
        thread using a pooled connection. It will be passed a database cursor 
        for your DB-API module of choice), and its results will be returned as 
        a Future. If running the method raises an exception, 
        the transaction will be rolled back. If the method returns a
        value, the transaction will be committed.

        Parameters
        ----------
        interaction: Callable function or object that implements the transaction logic
            Function or lambda function

        Returns
        -------
        A Future which will fire the return value of the function, or a failure message

        Notes
        -----
        The complete documentation, along with a complete set of
        initialization parameters, can be found at:
        https://twistedmatrix.com/documents/current/api/twisted.enterprise.adbapi.ConnectionPool.html#runInteraction
        
        Examples
        --------
        Running a SQL query (SELECT ... ):

        >>> def retrieve_tuples(cur, id, val1, val2):
        ...     stmt = """SELECT * FROM TABLE WHERE 
        ...               ID = %s AND COL1 = %s AND COL2 = %s"""
        ...     cur.execute(stmt, (id, val1, val2))
        ...     return cur.fetchall()

        >>> @tornado.gen.coroutine
        ... def execute_query(id, val1, val2):
        ...     conn = db.get_instance()
        ...     tuples = yield conn.run_transaction(retrieve_tuples, id, val1, val2)
        ...     tuples = map(lambda x: Object(x), tuples)
        ...     return tuples      
        ...
        >>> values = execute_query(ID, VAL1, VAL2)

        Running a SQL Data Manipulation Language Sentence 
        (DELETE, UPDATE, INSERT)

        >>> def delete_register(cur, register):
        ...     threshold = SOME_VALUE
        ...     stmt_1 = """SELECT col1 FROM TABLE WHERE ID = %s"""
        ...     stmt_2 = """DELETE FROM TABLE WHERE ID = %s"""
        ...     cur.execute(stmt_1, register['id'])
        ...     cmp_value = cur.fetchall()[0][0]
        ...     if cmp_value < threshold:
        ...        #Execute rollback
        ...        raise SomeException("Col1 value is less than threshold, cannot delete.")
        ...     cur.execute(stmt_2, register['id'])
        ...     #Commit the transaction
        ...     return register

        >>> @tornado.gen.coroutine
        ... def execute_delete(register):
        ...     conn = db.get_instance()
        ...     delete_confirm = yield conn.run_transaction(delete_register, register)
        ...     if delete_confirm is not None:
        ...        return "Successful!"
        ...     else:
        ...        return "An Error has ocurred"

        >>> msg = execute_delete(REGISTER)
        '''
        return self._pool.run_interaction(*args, **kwargs)

    def close(self):
        '''
        Close all pool connections and shutdown the pool.
        '''
        self._pool.close()

def initialize_db(*args, **kwargs):
    '''
    Initializes the single running instance of the DB connection pool.

    Notes
    -----
    For more information about the initialization parameters, 
    please refer to `dbconn`.DatabaseConnection.__init__
    reference
    '''
    global db_pool
    db_pool = DatabaseConnection(*args, **kwargs)

def get_instance():
    '''
    Returns the current running instance of the DB connection pool.

    Returns
    -------
    The current instance of DatabaseConnection present in the system 
    '''
    return db_pool

def close():
    """
    Close all pool connections and shutdown the pool.
    """
    global db_pool
    db_pool.close()
    return db_pool
