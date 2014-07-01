import subprocess, json, datetime, glob, os
import pandas as pd
import numpy as np
from AnxPy import Console
from AnxPy.environ import DW_PROD, DW_CTEST, DW_SAND
from anxapi import *
from link import lnk
from abc import ABCMeta, abstractmethod


def createReader(config):
    if config['type'] == 'hdfs':
        return HadoopFeedReader(config)
    elif config['type'] == 'API':
        return ApiReader(config)
    elif config['type'] == 'database':
        return DatabaseReader(config)
    elif config['type'] == 'CSV':
        return CsvReader(config)
    else:
        raise Exception('FeedReader not found')


class FeedReader:
    ''' An abstract class for reading in AppNexus data from a variety of sources into a flat dataframe. 
    Implement:
    r = createReader(config)
    dataframe = r.read()
    '''
    __metaclass__ = ABCMeta

    def __init__(self, config):
        self.config = config

    def __str__(self):
        print json.dumps(self.config, indent=2)

    @abstractmethod
    def read(self):
        pass

    def get_config(self):
        return self.config

    def set_config(self, config):
        self.config = config
        return True


class HadoopFeedReader(FeedReader):
    ''' 
    Read in data from Hadoop into a 2D table (dataframe)

    example config::
    {
        "type":"hdfs",
        "location":"/dv/domain_hourly_blocks/",
        "filter":{
            "days_ago":1
        }
    }
    '''
    def __init__(self, config):
        self.config = config

    def day_calc(self, n_days):
        ''' get date (n day ago from now, in UTC) '''
        import datetime
        d = datetime.datetime.utcnow() - datetime.timedelta(days=n_days)
        return d.strftime("%Y/%m/%d")

    def deleteTmp(self):
        ''' remove previous files from tmp/ if it exists '''
        if os.path.exists('tmp/'):
            filelist = glob.glob("tmp/*")
            for f in filelist:
                os.remove(f)
        else:
            os.makedirs('tmp/')

    def getHDFS(self):
        ''' Connect to HDFS and get all of the compressed part files '''
        # hdfs dfs -get /dv/domain_hourly_blocks/2014/04/16/part-r-00000.lzo | lzop -dc
        
        if 'days_ago' in self.config['filter']:
            daystr = self.day_calc(self.config['filter']['days_ago'])
        else:
            daystr = self.day_calc(1)

        loc = self.config['location']

        cmd1 = "hdfs dfs -get " + loc + daystr + "/* tmp/"
        args1 = cmd1.split()
        p1 = subprocess.Popen(args1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p1.communicate()
        print "Hadoop dump:", stdout, stderr
        print

    def decompressLZO(self):
        ''' find all file parts, decompress .lzo files '''
        with open("tmp/.pig_header", "r") as h:
            headers = h.read()
            headers = headers[:-1]
            headers = headers.split(",")

        data = []
        filelist = glob.glob("tmp/*.lzo")
        with open("error.log", "ab") as err:
            err.write(self.day_calc( self.config['filter']['days_ago'] ) + ":")
            for filepart in filelist:
                cmd2 = "/usr/bin/lzop -dcf"
                args2 = cmd2.split()
                
                with open(filepart, "rb") as part:
                    p2 = subprocess.Popen(args2, stdin=part, stdout=subprocess.PIPE, stderr=err)
                    (stdout, stderr) = p2.communicate()
                    segment = stdout.split("\n")
                    for row in segment:
                        row = row[:-1]
                        row = row.split(",")
                        data.append(row)
                    del segment, stdout

        print "headers: " + str(headers)

        df = pd.DataFrame(data, columns=headers)
        return df

    def textHDFS(self):
        ''' Connect to HDFS and process all of the snappy part files '''
        # hdfs dfs -ls /dv/domain_hourly_blocks/2014/06/05/
        # hdfs dfs -text /dv/domain_hourly_blocks/2014/06/05/part-r-00000.snappy

        if 'days_ago' in self.config['filter']:
            daystr = self.day_calc(self.config['filter']['days_ago'])
        else:
            daystr = self.day_calc(1)

        loc = self.config['location']

        cmd1 = "hdfs dfs -ls " + loc + daystr + "/"
        p1 = subprocess.Popen( cmd1.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p1.communicate()
        print stderr

        # get snappy files
        data_files = stdout.replace('\n',' ')
        data_files = data_files.split()
        data_files = [f for f in data_files if '.snappy' in f]
        # sort by part number
        data_files.sort()

        # get headers
        cmd2 = "hdfs dfs -text " + loc + daystr + "/.pig_header"
        p2 = subprocess.Popen( cmd2.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (headers, stderr) = p2.communicate()
        print stderr
        # create empty DataFrame
        headers = headers.replace('\n','')
        #headers = headers + ','
        headers = headers.split(',')
        data_df = pd.DataFrame(columns=headers)

        # retreive data
        for data_file in data_files:
            cmd3 = "hdfs dfs -text " + data_file
            # forced to use temp csv file in orderto get nice unit conversion
            try:
                temp_file = open('temp.csv','w')
                p3 = subprocess.Popen( cmd3.split(), stdout=temp_file, stderr=subprocess.PIPE)
                (s, stderr) = p3.communicate()
                print stderr
                temp_file.close()
            except Exception, e:
                print "Skipping: " + data_file
                print str(e)
                # logger.exception
                continue

            # convert string to list of lists
            #data_part = [row.split(',') for row in stdout.split('\n') if row]
            # convert list of lists (with headers) to a list of dictionaries
            #df = [ {data_part[0][i]:row[i] for i in range(len(row))} for row in data_part[1:] ]
            #df = pd.DataFrame(df)
            # Pretty harsh unit conversion, watch for columns with all NaN
            #df = df.convert_objects(convert_numeric=True)

            # read and append csv
            df = pd.read_csv('temp.csv', index_col=False, names=headers, header=None)            
            data_df = data_df.append(df, ignore_index=True)

        return data_df

    def read(self):
        # read in .snappy files
        df = self.textHDFS()
        return df



class ApiReader(FeedReader):
    ''' 
    Read in data from the API into a 2D table (dataframe)

    example config::
    {
        "type":"API",
        "environment":"dw-prod",
        "service":"domain-list",
        "filter":{
            "ids":[123456],
            "member_id":958
        },
        "field_name":"domains",
        "field_type":"list",
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
        ''' Retreives the object json 
        '''
        service = self.config['service']
        if self.console:
            # convert dashs to camelcase
            service = ''.join( [word[0].upper() + word[1:] for word in service.split('-')] )
            # same as: service = console.DomainList
            service = getattr(console, service)
            api_collection = service.get(filter=self.config['filter'])
            api_objects = api_collection.get_all()
            api_objects = [ api_object.data for api_object in api_objects ]

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

    def create_df(self, api_objects):
        ''' Creates df from field_name and object jsons
        '''
        data = []
        save_list = ['id', 'advertiser_id', 'publisher_id', 'member_id', 'line_item_id', 'insertion_order_id']

        for api_object in api_objects:
            row = {}
            for save_item in save_list:
                if save_item in api_object:
                    row[save_item] = api_object[save_item]

            field = api_object[ self.config['field_name'] ]
            
            if isinstance(field, (list, tuple)):
                for item in field:
                    row[ self.config['field_name'] ] = item
                    data.append(row)
            else:
                row[ self.config['field_name'] ] = field
                data.append(row)

        return pd.DataFrame(data)


    def read(self):
        api_objects = self.get_objects()
        df = self.create_df(api_objects)
        return df



class DatabaseReader(FeedReader):
    ''' 
    Read in data from a database into a 2D table (dataframe), using Link

    example config::
    {
        "type":"database",
        "db":"vertica",
        "query":"select * from agg_dw_advertiser_publisher_analytics_adjusted limit 30;"
    }
    '''
    def __init__(self, config):
        self.config = config

    def read(self):
        db = getattr(lnk.dbs, self.config['db'])
        df = db.select_dataframe(self.config['query'])
        return df

class CsvReader(FeedReader):
    '''
    Read in data from a csv file into a 2D table (dataframe), using a native pandas function

    example config::
    {
        "type":"CSV",
        "filename":"data.csv"
    }
    '''
    def __init__(self, config):
        self.config = config

    def read(self):
        db = pd.read_csv( self.config['filename'], index_col=False )
        return df



