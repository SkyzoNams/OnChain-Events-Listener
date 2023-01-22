import pytest
import json
import eth_event
import time
import logging
from src.events_handler import Events_Listener
from web3 import Web3

def get_dict_file(file_path):
    """
    Helper function to return the contents of a file as a dictionary
    """
    with open(file_path, "r") as f:
        return json.loads(f.read())
    
class TestEventsListener:
    @pytest.fixture
    def listener(self):
        return Events_Listener()

    def test_init(self, listener):
        assert listener.file_path == "./files/recorded_block.txt"
        assert listener.contract_address == "0xBAac2B4491727D78D2b78815144570b9f2Fe8899"
        assert listener.web3 is None

    def test_get_topic_map(self, listener):
        with open('./files/abi.json') as json_file:
            expected_topic_map = eth_event.get_topic_map(json.load(json_file))
        assert listener.topic_map == expected_topic_map

    def test_connect_to_provider(self, listener):
        web3 = listener.connect_to_provider()
        assert isinstance(web3, Web3)
        assert web3.isConnected() == True

    def test_provider(self, listener):
        web3 = listener.provider()
        assert isinstance(web3, Web3)
        assert web3.isConnected() == True

    