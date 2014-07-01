import subprocess, json, datetime, glob, os, unicodedata
import pandas as pd
import numpy as np
from AnxPy import Console
from AnxPy.environ import DW_PROD, DW_CTEST, DW_SAND
from anxapi import *
from anxtools import *
from link import lnk
from abc import ABCMeta, abstractmethod


def createWriter(config):
    if config['type'] == 'API':
        return ApiWriter(config)
    elif config['type'] == 'database':
        return DatabaseWriter(config)
    elif config['type'] == 'CSV':
        return CsvWriter(config)
    elif config['type'] == 'stdout':
        return StdoutWriter(config)
    elif config['type'] == 'mail':
        return MailWriter(config)
    elif config['type'] == 'chart':
        return ChartWriter(config)
    else:
        raise Exception('FeedWriter not found')


class FeedWriter:
    ''' An abstract class for writing AppNexus data to a variety of locations from a flat dataframe. 
    Implement:
    w = createWriter(config)
    result = w.write(dataframe)
    '''
    __metaclass__ = ABCMeta

    def __init__(self, config):
        self.config = config

    def __str__(self):
        print json.dumps(self.config, indent=2)

    @abstractmethod
    def write(self, df):
        pass

    def get_config(self):
        return self.config

    def set_config(self, config):
        self.config = config
        return True


class ApiWriter(FeedWriter):
    ''' 
    Read in data from Hadoop into a 2D table (dataframe)

    example config::
    {
        "type":"API",
        "environment":"dw-prod",
        "service":"domain-list",
        "filter":{
            "id":[123456],
            "member_id":958
        },
        "column_name":"site_domain",
        "field_name":"domains",
        "field_type":"list",
        "action":"append"
    }
    '''
    def __init__(self, config):
        self.config = config

        if self.config['environment'] in ['prod', 'dw-prod']:
            self.base = 'dw-prod'
            self.console = Console(DW_PROD)
        elif self.config['environment'] in ['sand', 'dw-sand']:
            self.base = 'dw-sand'
            self.console = Console(DW_SAND)
        elif self.config['environment'] in ['ctest', 'dw-ctest']:
            self.base = 'dw-ctest'
            self.console = Console(DW_CTEST)
        elif self.config['environment'] in ['api-prod', 'api-sand', 'api-ctest']:
            self.base = self.config['environment']
            self.console = None
        else:
            raise Exception('Environment not found')

    def querystr(self):
        ''' convert filter into query string params
        '''
        if 'api' in self.base:
            qstr = '/'
        else:
            qstr = '?'

        for key, val in self.config['filter']:
            qstr += str(key) + '=' + str(val) + '&'
        qstr = self.config['service'] + qstr[:-1]
        return qstr

    def get_objects(self):
        service = self.config['service']
        if self.console:
            # convert dashs to camelcase
            service = ''.join( [word[0].upper() + word[1:] for word in service.split('-')] )
            print service
            # same as: service = console.DomainList
            service = getattr(self.console, service)
            api_collection = service.get(filter=self.config['filter'])
            print api_collection.num_elements
            api_objects = api_collection.get_all()

        else:
            response = anx_get(self.base, self.querystr())

            if 'error' in response or 'error_code' in response:
                raise Exception(json.dumps(response, indent=2))
            else:
                response = anx_get_all(self.base, service + querystr)
                if response['status'] != 'OK':
                    raise Exception('Unknown API error')
                else:
                    if service in response:
                        api_objects = [response[service]]
                    elif (service+'s') in response:
                        api_objects = response[service+'s']
                    else:
                        raise Exception('Unknown API error')

        return api_objects


    def load_data(self, values, api_objects):
        for api_object in api_objects:
            # extract json fields
            if self.console:
                api_dict = api_object.data
            else:
                api_dict = {}
                save_list = ['id', 'advertiser_id', 'publisher_id', 'member_id', 'line_item_id', 'insertion_order_id']
                for save_item in save_list:
                    if save_item in api_object:
                        api_dict[save_item] = api_object[save_item]

            if self.config['field_name'] not in api_dict:
                api_dict[ self.config['field_name'] ] = None

            # load differently based on the field type
            if self.config['field_type'] == 'list':
                if self.config['action'] == 'append':
                    if not api_dict[self.config['field_name']]:
                        api_dict[ self.config['field_name'] ] = []
                    api_dict[ self.config['field_name'] ].extend(values)
                else:
                    api_dict[ self.config['field_name'] ] = values
            else:
                raise Exception('Unsupported field types')


            fail = []
            success = []
            # save API object
            if self.console:
                try:
                    api_object.save()
                except Exception, e:
                    # logger.debug(str(e))
                    fail.append(api_object.id)
                else:
                    success.append(api_object.id)
            else:
                api_object = { self.config['service'] : api_dict }
                response = anx_put( self.base, self.querystr(), json.dumps(api_object) )

                if 'error' in response or 'error_code' in response:
                    #logger.debug(json.dumps(response, indent=2))
                    fail.append(api_dict['id'])
                else:
                    if response['status'] != 'OK':
                        #logger.debug('Unknown API error')
                        fail.append(api_dict['id'])
                    else:
                        success.append(api_dict['id'])

        return (success, fail)


    def write(self, df):
	if not df:
            return False

        values = df[ self.config['column_name'] ].values.tolist()
        api_objects = self.get_objects()

        (success, fail) = self.load_data(values, api_objects)
        # logger.debug('Success: ' + str(success))
        # logger.debug('Failed: ' + str(fail))
        return (len(fail) == 0)



class DatabaseWriter(FeedWriter):
    ''' 
    Write data to a database from a 2D table (dataframe), using Link

    example config::
    {
        "type":"database",
        "db":"localsql",
        "table":"testdb"
    }
    '''
    def __init__(self, config):
        self.config = config

    # based off of David Blaikie's fiba script library, dfutils
    def insert_df(df, table_name, chunksize=100, dbconnection=None, doinsert=False, encoding='latin-1'):
        """
        Given a dataframe, table name and link db connection, insert data or return insert SQL. 
        Custom module created by David Blaikie.

        Args:
            df:  dataframe to be inserted
            table_name: name of table
            chunksize:  size of SQL insert chunk
            dbconnection:  available db connection object
            doinsert:  execute insert quer(ies).  If false, returns SQL statement or list of SQL statements
            encoding:  only if a column's input data is unicode, encode to a string using this encoding

        Returns:
            string query, or list of string queries (if chunksize)
        """
        return_chunks = False
        df = df.copy()
        if doinsert and not dbconnection:
            exit("dataframe2insertsql:  when doinsert=True, must supply dbconnection object")

        def stringify_for_sql(i, this_type=None):
            """stringify, SQL-escape an object, adding quotes if it is a string"""
            if isinstance(i, basestring):
                if isinstance(i, unicode):
                    try:
                        i = i.encode(encoding)
                    except UnicodeEncodeError:
                        i = unicodedata.normalize('NFKD', i).encode(encoding, 'ignore')

                if dbconnection:
                    i = dbconnection.escape_string(i)
                else:
                    import MySQLdb
                    MySQLdb.escape_string(SQL)

            if isinstance(i, basestring):
                i = "'" + str(i) + "'"
            else:
                try:
                    if i is None or np.isnan(i):
                        i = 'NULL'
                except TypeError:
                    pass
                i = str(i)

            return i


        # string escape every element in the dataframe
        df = df.applymap(stringify_for_sql)

        if chunksize:
            return_chunks = True

        if (not chunksize) or (chunksize >= len(df)):
            chunksize = len(df)

        sql_chunk_queries = []
        errant_queries = []
        headers = df.columns.values.tolist()

        for chunk in ( df[x:x+chunksize] for x in xrange(0, len(df), chunksize) ):
            query = "INSERT INTO %s\n" % table_name
            query += "(" + ','.join(headers) + ")"
            query += " VALUES \n"

            chunk = chunk.to_records(index=False)
            for tuples in chunk:
                row = "(" + ','.join(tuples) + "),\n"
                query += row
            query = query[:-2] + "\n"
            query += " ON DUPLICATE KEY UPDATE \n"

            for col in headers:
                query += "%s=VALUES(%s), " % (col, col)
            query = query[:-2]
            query += ";"

            if doinsert:
                # execute insert query
                # if it fails for whatever reason, split into individual queries and execute 
                try:
                    dbconnection.execute(query)
                except Exception, e:
                    splt = query.splitlines()
                    foqueries = splt[2: len(splt) - 2]
                    beginning = ''.join( splt[0: 2] )
                    ending = ''.join( splt[len(splt) - 2: len(splt)] )
                    print 'chunk query failed:  %s queries retrieved' % len(foqueries)
                    print 'chunk query error:  \n%s' % str(e)
                    print
                    for foquery in foqueries:
                        try:
                            foquery = foquery.rstrip(',')
                            foquery = beginning + " " + foquery + " " + ending
                            dbconnection.execute(foquery)
                        except Exception, e:
                            errant_queries.append((foquery, str(e)))

                dbconnection.commit()

            else:
                sql_chunk_queries.append(query)

        if errant_queries:
            print "permanently failed queries: ", len(errant_queries)
            for query, this_exception in errant_queries:
                print query
                print this_exception
                print

        if doinsert:
            return True
        else:
            if len(sql_chunk_queries) > 1 or return_chunks:
              return sql_chunk_queries
            return sql_chunk_queries[0]


    def write(self, df):
        if not df:
            return False

        dbconn = getattr(lnk.dbs, self.config['db'])
        result = self.insert_df(df, self.config['table'], dbconnection=dbconn)
        return result


class CsvWriter(FeedWriter):
    '''
    Write data to a csv file from a 2D table (dataframe), using a native pandas function

    example config::
    {
        "type":"CSV",
        "filename":"data.csv"
    }
    '''
    def __init__(self, config):
        self.config = config

    def write(self, df):
        if not df:
            return False

        if self.config.has_key('column_name'):
            df = df[ self.config['column_name'] ]

        df.to_csv( self.config['filename'], index=False )
        return True


class StdoutWriter(FeedWriter):
    '''
    Write data to stdout from a 2D table (dataframe)

    example config::
    {
        "type":"stdout"
    }
    '''
    def __init__(self, config):
        self.config = config

    def write(self, df):
        if not df:
            return False

        if self.config.has_key('column_name'):
            df = df[ self.config['column_name'] ]

        print df
        return True


class MailWriter(FeedWriter):
    '''
    Write data to a csv file from a 2D table (dataframe), and email it to recipients
    Uses the anxtools library

    example config::
    {
        "type":"mail",
        "filename":"data.csv",
        "recipients": [
            "babraham@appnexus.com"
        ],
        "sender":"babraham@appnexus.com",
        "subject":"New Dataframe",
        "body":"Here is a new csv file of data."
    }
    '''
    def __init__(self, config):
        self.config = config

    def write(self, df):
        if not df:
            return False

        if self.config.has_key('column_name'):
            df = df[ self.config['column_name'] ]

        df.to_csv( self.config['filename'] )
        # send email
        send_email( self.config['recipients'], \
                send_from= self.config['sender'], \
                reply_to= self.config['sender'], \
                subject= self.config['subject'], \
                body= self.config['body'], \
                attachment_paths= self.config['filename'] )
        return True


class ChartWriter(FeedWriter):
    '''
    Write data to a png chart from a 2D table (dataframe)
    Uses the anxtools library. Axises must be numerical values.

    example config::
    {
        "type":"chart",
        "filename":"chart.png",
        "x_column_name":"x_axis",
        "y_column_name":"y_axis"
    }
    '''
    def __init__(self, config):
        self.config = config

    def write(self, df):
        if not df:
            return False

        # assemble axises. Must be numerical values.
        x_axis = df[ self.config['x_column_name'] ].values.tolist()
        y_axis = df[ self.config['y_column_name'] ].values.tolist()
        
        make_chart(x_axis, y_axis, self.config['filename'])
        return True



