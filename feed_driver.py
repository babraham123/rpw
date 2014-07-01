'''
Reads, processes, and writes flat data types using the FeedReader, FeedWriter, and DataProcessor classes.
Visit their documentation for example configuration files.

Execution:
python feed_driver.py -c config.json

by Bereket Abraham
'''

from reader import *
from processor import *
from writer import *
import argparse, json

if __name__ == "__main__":
    
    helpdesc = '''
    Reads, processes, and writes flat data types using the FeedReader, FeedWriter, and DataProcessor classes.
    Visit their documentation for example configuration files.
    '''
    parser = argparse.ArgumentParser(description=helpdesc)
    # These will be captured from the command line when the script is run
    parser._optionals.title = "For help"
    required_group = parser.add_argument_group("REQUIRED")
    required_group.add_argument('-c',dest='config', type=str, help='Configuration File, in JSON')
    #optional_group = parser.add_argument_group("OPTIONAL")
    #optional_group.add_argument('-l',dest='litem', type=int, nargs='?', default=None, help='Line item id')

    # Parse the arguments and store the collection in 'args'
    args = parser.parse_args()
    config_file = args.config

    # configurations to handle new aggregations, thresholds, and blacklists
    with open(config_file) as f:
        feeds = json.load(f)
        feeds = feeds["feeds"]

    for feed in feeds:
        print
        print 'Starting feed: ' + feed['name'] + ' ....'

        # combine sources, must have same headers
        df = None
        for source in feed['sources']:
            r = createReader(source)
            df_part = r.read()
            # merge dfs!!! by row or by column??
            # only one source for now
            if df is not None:
                df = df.append(df_part, ignore_index=True)
            else:
                df = df_part

        processor = DataProcessor(feed)
        df = processor.operate(df)
        df = processor.select(df)

        for dest in feed['destinations']:
            w = createWriter(dest)
            result = w.write(df)
            print result

        ##

