from src.events_handler import Events_Listener
import argparse
import logging
logging.basicConfig(format="%(asctime)s: %(levelname)s - %(message)s", level=logging.INFO)

parser = argparse.ArgumentParser(description='Seek for contract activity')
parser.add_argument('-from', dest='start_block', help='the block to start to listen from.', required=False)
parser.add_argument('-to', dest='end_block', help='the block to end to listen.', required=False)

def main():
    try:
        if parser.parse_args().start_block is None and parser.parse_args().end_block is None:
            Events_Listener().fetch_events()
        elif parser.parse_args().start_block is not None:
            Events_Listener().fetch_events_in_blocks(int(parser.parse_args().start_block), parser.parse_args().end_block if parser.parse_args().end_block is None else int(parser.parse_args().end_block)) 
        else:
            logging.error('please define a start_block -from or try to execute it without any parameter')
    except Exception as e:
        raise e    

    
    
if __name__ == "__main__":
    main()