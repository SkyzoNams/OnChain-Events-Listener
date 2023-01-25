import logging
import time
import threading
import eth_event
from web3 import Web3
from src.utils import get_dict_file
from src.database_manager import DataBaseManager
from dotenv import load_dotenv
load_dotenv()
logging.addLevelName(24, "CONNECTION")
logging.addLevelName(25, "BLOCKS INFO")
logging.addLevelName(26, "DATABASE")
from datetime import datetime, timedelta
import requests

class Events_Listener():
    def __init__(self):
        # Contract ABI for decoding events
        self.contract_abi = get_dict_file("./files/abi.json")
        self.topic_map = eth_event.get_topic_map(self.contract_abi)
        # Contract address to listen for events on
        self.contract_address = "0xBAac2B4491727D78D2b78815144570b9f2Fe8899"
        # Web3 instance for connecting to the provider
        self.web3 = None
        # Thread pool for fetch_events_in_blocks
        self.threads = []
        self.max_threads = 8
        self.infura_key = "178f1d53d56842baaf55e41ec9efec61"
        self.db_manager = DataBaseManager()
        self.total_supply = self.get_total_supply()
        self.endpoint = "https://api.etherscan.io/api"
        self.etherscan_api_key = "S6S2P8NCWF2DWVGKJB46ZY3G46TDIRFUIF"

    def connect_to_provider(self):
        """
        @Notice: This function is used to connect the contract to the web3 provider
        @Dev: We first create a web3 instance linked to the Infura provider and then we instantiate
        the contract using the web3 provider
        """
        try:
            self.web3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/" + self.infura_key))
            tries = 5
            # Check for connection and retry if unsuccessful
            while self.web3.isConnected() is False and tries >= 0:
                logging.info("waiting for web3 connection...")
                time.sleep(2)
                self.web3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/" + self.infura_key))
                tries -= 1
            return self.web3
        except Exception as e:
            raise e

    def provider(self):
        """
        @Notice: This function will check for existing web3 connection or connect to the provider
        @Return: web3 instance
        """
        if self.web3 is None or self.web3.isConnected() is False:
            return self.connect_to_provider()
        return self.web3

    def fetch_events(self):
        """
        @Notice: This function will listen for new blocks to explore
        @Dev: We first define from which block to start the exploration.
        We call the fetch_events_in_blocks method to explore the blocks.
        """
        try:
            current_block_number = None
            while 42:
                current_block_number, last_block_number = self.get_current_block_number(current_block_number)
                logging.log(25, "from #" + str(current_block_number) + " to #" + str(last_block_number))
                current_block_number = self.fetch_events_in_blocks(current_block_number, last_block_number)
                self.waiting_for_new_blocks( current_block_number, last_block_number)
        except Exception as e:
            raise e

    def fetch_events_in_blocks(self, current_block_number: int, last_block_number: int):
        """
        This function iterates over block numbers to fetch all the events for the token transfer event 
        between `current_block_number` and `last_block_number`. It handles the 1000 results limit from the 
        etherscan api by incrementing the from_block and to_block values regarding this limit.
        @param current_block_number: the last block number that was processed
        @param last_block_number: the last block number on the blockchain
        If last_block_number is None, the function will retrieve the last block number on the blockchain.
        Then, it makes a request to the Etherscan API to fetch all the events between the last processed block and the last block
        The function then decode the response, and process each event using the handle_event method
        @return: int : the last processed block number
        """
        try:
            if last_block_number is None:
                last_block_number = self.get_last_block_number()

            # Initialize variables for pagination
            results_per_page = 1000
            current_page = 1
            start_value = current_block_number
            end_value = last_block_number
            from_block = (current_page - 1) * results_per_page + start_value
            to_block = current_page * results_per_page + start_value

            while current_block_number <= last_block_number:
                response = requests.get(self.endpoint, params=self.get_api_params(from_block, to_block))
                for event in response.json()["result"]:
                    self.handle_event(event)
                    
                current_page += 1
                current_block_number = to_block + 1
                from_block = to_block + 1
                to_block = to_block + results_per_page + 1
                if to_block > last_block_number:
                    to_block = last_block_number
                    
            return end_value + 1
        except Exception as e:
            raise e
        
    def get_api_params(self, from_block, to_block):
        """
        @Notice: This function returns the parameters required to query the Etherscan API for logs
        @param from_block: The block number to start querying from.
        @param to_block: The block number to stop querying at.
        @return: A dictionary object containing the parameters required for the API call.
        """
        return {
                "module": "logs",
                "action": "getLogs",
                "fromBlock": from_block,
                "toBlock": to_block,
                "address": self.contract_address,
                "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # Transfer event signature
                "apikey": self.etherscan_api_key
            }

    def waiting_for_new_blocks(self, current_block_number, last_block_number):
        """
        @Notice: This function is used to wait for new blocks to be mined and return the latest block number
        @param: current_block_number: the last block number that was processed
        @param: last_block_number: the last block number available
        @Dev: We keep checking for new blocks until the last_block_number is less than current_block_number.
        """
        logging.log(25, "waiting for not explored blocks...")
        while last_block_number < current_block_number:
            last_block_number = self.get_last_block_number()
        logging.log(25, "new blocks found, will explore now from #" + str(current_block_number) + " to #" + str(last_block_number))

    def get_current_block_number(self, current_block_number):
        """
        @Notice: This function will return the last processed block number from the recorded block file
        @param: current_block_number: the last processed block number
        @return: The last processed block number and the current block number
        """
        try:
            last_block_number = self.get_last_block_number()
            if current_block_number is None:
                current_block_number = last_block_number
            return current_block_number, last_block_number
        except Exception as e:
            raise e

    def get_last_block_number(self):
        """
        @Notice: This function will return the current block number from the provider
        @return: The current block number
        @Dev: We call the get_block method on the web3 provider and return the result
        """
        return int(self.provider().eth.get_block('latest')['number'])

    def handle_event(self, event):
        """
        @Notice This function handles events and stores the balance of the addresses in the user_balance table and the event information in transfer_events.
        @param event: The event that needs to be handled
        @Dev:
            - Uses the insert_user_balance function to store the balance of the addresses
            - Uses the insert_event function to store the event information
        """
        from_address = event['topics'][1][-40:]
        # get the to address from the topics
        to_address = event['topics'][2][-40:]
        # get the transaction hash from the result
        transaction_hash = event['transactionHash']
        # get the timestamp from the result
        timestamp = datetime.fromtimestamp(int(event['timeStamp'], 16))
        # get the block number from the result
        block_number = int(event['blockNumber'], 16)
        self.insert_event(event, transaction_hash, timestamp)
        self.insert_user_balance(from_address, transaction_hash, timestamp, block_number)
        self.insert_user_balance(to_address, transaction_hash, timestamp, block_number)
                
    def get_balance(self, address, block_number):
        """
        @Notice: This function returns the balance the token for a specific address
        @param address: The address you want to get the balance of
        @return: The balance of the address in wei unit
        """
        # Call the function and get the balance
        balance_in_wei = self.provider().eth.get_balance(Web3.toChecksumAddress(address), block_number)
        balance_in_ether = self.web3.fromWei(balance_in_wei, 'ether')
        return balance_in_ether
    
    def insert_user_balance(self, address, transaction_hash, timestamp, block_number):
        """
        @Notice: This function insert or updates the balance of a specific address 
        in the user_balance table.
        @param address: The address you want to insert/update the balance
        @param transaction_hash: The event transaction_hash
        @param block_number: current block number
        @param timestamp: the transaction timestamp
        @Dev: This function uses the get_balance function to get the balance of the address and uses
        calculate_weekly_change function to get the weekly changes percentage value
        """
        if address == "0000000000000000000000000000000000000000":
            return None
        balance = self.get_balance(address, block_number)
        weekly_change = self.calculate_weekly_change(address, balance, timestamp)
        self.db_manager.execute(query="""INSERT INTO user_balance (balance, address, transaction_hash, transaction_date, total_supply_pct, weekly_change_pct)
            SELECT %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM user_balance
                WHERE address = %s AND transaction_hash = %s);""", item_tuple=(balance, address, transaction_hash, timestamp, self.calculate_total_supply_pct(balance), weekly_change, address, transaction_hash))
        logging.log(26, "user " + str(address) + " balance inserted from block #" + str(block_number))
    
    def calculate_total_supply_pct(self, balance):
        """
        @Notice: to calculate the total supply percentage of a given balance.
        @Dev: This function calculates the percentage of a given balance against the total supply.
        @param balance: the balance to calculate the total supply percentage for.
        @return: the total supply percentage of the given balance.
        """
        if balance == 0:
            return 0
        total_supply_pct = (balance / self.total_supply) * 100
        return total_supply_pct
      
    def calculate_weekly_change(self, address, balance, timestamp):
        """
        @Notice: to calculate the weekly change of a balance for a given address.
        @Dev: This function calculates the percentage change of a balance for a given address 
        for a given date compared to the previous week.
        @param address: the address to calculate the weekly change for.
        @param balance: the current balance of the user address.
        @param timestamp: the transaction timestamp.
        @return: the weekly change of the balance for the given address.
        """
        new_timestamp = timestamp - timedelta(days=7)
        records = self.db_manager.select_all(query="""SELECT balance FROM user_balance
            WHERE address = '""" + address + """' AND transaction_date <= '""" + str(new_timestamp) + """' ORDER BY transaction_date DESC LIMIT 1;""")

        if len(records) == 0 or len(records[0]) == 0 or records[0][0] == None:
            return None
        elif float(balance) == 0.0 and records[0][0] == 0.0:
            return 0.0
        elif records[0][0] == 0.0:
            return None
        
        change_cp_to_last_week = ((float(balance) - records[0][0]) / records[0][0]) * 100
        return change_cp_to_last_week
      
    def insert_event(self, event, transaction_hash, timestamp):
        """
        @Notice: This function insert the event record in the transfer_events table.
        @param event: The event you want to insert
        @param transaction_hash: The event transaction_hash
        @param timestamp: the transaction timestamp.
        """
        self.db_manager.execute(query="""INSERT INTO transfer_events (event_args, transaction_hash, event_name, contract_address, transaction_date)
            SELECT %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM transfer_events
                WHERE transaction_hash = %s);""", item_tuple=(str(event), transaction_hash, "Transfer", self.contract_address, timestamp, transaction_hash))
        logging.log(26, "transfer events record inserted from transaction hash " + str(transaction_hash))

    def get_total_supply(self):
        """
        @Notice: This function returns the total supply of the token contract
        @Dev: This function uses the web3.eth.contract function to get the contract object
        @return: total supply of the token in wei unit
        """
        contract = self.provider().eth.contract(address=self.contract_address, abi=self.contract_abi)
        # The function that returns the total supply
        total_supply_function = contract.functions.totalSupply()
        # Call the function and get the total supply
        total_supply = total_supply_function.call()
        return self.web3.fromWei(total_supply, 'ether')