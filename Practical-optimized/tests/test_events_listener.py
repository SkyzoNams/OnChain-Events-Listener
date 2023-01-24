import pytest
import json
import eth_event
from src.events_handler import Events_Listener
from web3 import Web3
import datetime
import time
import threading
from mock import MagicMock
from decimal import Decimal
from src.database_manager import DataBaseManager
from hexbytes import HexBytes
import requests_mock

@pytest.fixture
def events_listener():
    return Events_Listener()

def test_init(events_listener):
    assert events_listener.contract_address == "0xBAac2B4491727D78D2b78815144570b9f2Fe8899"
    assert isinstance(events_listener.db_manager, DataBaseManager)
    assert events_listener.contract_address == "0xBAac2B4491727D78D2b78815144570b9f2Fe8899"
    assert events_listener.threads == []
    assert events_listener.max_threads == 8
    assert events_listener.infura_key == "178f1d53d56842baaf55e41ec9efec61"
    assert events_listener.total_supply == 16969696969
    assert events_listener.endpoint == "https://api.etherscan.io/api"

def test_get_topic_map(events_listener):
    with open('./files/abi.json') as json_file:
        expected_topic_map = eth_event.get_topic_map(json.load(json_file))
    assert events_listener.topic_map == expected_topic_map

def test_connect_to_provider(events_listener):
    web3 = events_listener.connect_to_provider()
    assert isinstance(web3, Web3)
    assert web3.isConnected() == True
    assert web3 is not None

def test_provider(events_listener):
    web3 = events_listener.provider()
    assert isinstance(web3, Web3)
    assert web3.isConnected() == True
    assert web3 is not None

def test_get_last_processed_block_number(events_listener):
    last_processed_block_number, last_block_number = events_listener.get_last_processed_block_number(None)
    assert last_processed_block_number == last_block_number
    last_processed_block_number, last_block_number = events_listener.get_last_processed_block_number(100000)
    assert last_processed_block_number != events_listener
    assert isinstance(last_processed_block_number, int)
    assert isinstance(last_block_number, int)

def test_get_last_block_number(events_listener):
    last_block_number = events_listener.get_last_block_number()
    assert isinstance(last_block_number, int)
    assert last_block_number is not None
    assert last_block_number > 16470575

@pytest.mark.timeout(5) # Set timeout to 5 seconds
def test_fetch_events(events_listener, monkeypatch):
    stop_flag = threading.Event()
    monkeypatch.setattr(events_listener, 'fetch_events', lambda: stop_flag.wait())
    thread = threading.Thread(target=events_listener.fetch_events)
    thread.start()
    time.sleep(3)
    stop_flag.set()
    thread.join()
    assert True # If the function runs without any errors, the test should pass

        
def test_fetch_events_in_blocks(events_listener):
    last_processed_block_number = 13140651
    last_block_number = 13150651
    last_processed_block_number = events_listener.fetch_events_in_blocks(last_processed_block_number, last_block_number)
    assert last_processed_block_number == last_block_number

@pytest.mark.timeout(5) # Set timeout to 5 seconds
def test_waiting_for_new_blocks(events_listener):
    last_block_number = events_listener.get_last_block_number()
    events_listener.waiting_for_new_blocks(last_block_number, last_block_number)
    assert True # If the function runs without any errors, the test should pass

def test_get_total_supply(events_listener):
    total_supply = events_listener.get_total_supply()
    assert isinstance(total_supply, Decimal)
    assert total_supply == 16969696969

def test_insert_event(events_listener):
    mock_db = MagicMock()
    events_listener.db_manager = mock_db
    event_data = {"name": "event_name", "data": []}
    events_listener.insert_event(event_data, "0xfc0ad0aa9683b397c1c4da5c11276c630ef7188c9b111086af1298c066305208", datetime.datetime.fromtimestamp(1674488815.6867409))
    mock_db.execute.assert_called_once_with(query="""INSERT INTO transfer_events (event_args, transaction_hash, event_name, contract_address, transaction_date)\n            SELECT %s, %s, %s, %s, %s\n            WHERE NOT EXISTS (\n                SELECT 1 FROM transfer_events\n                WHERE transaction_hash = %s);""", item_tuple=("{'name': 'event_name', 'data': []}", '0xfc0ad0aa9683b397c1c4da5c11276c630ef7188c9b111086af1298c066305208', 'Transfer', '0xBAac2B4491727D78D2b78815144570b9f2Fe8899', datetime.datetime(2023, 1, 23, 16, 46, 55, 686741), '0xfc0ad0aa9683b397c1c4da5c11276c630ef7188c9b111086af1298c066305208'))

def test_get_balance(events_listener):
    address = "0x30741289523c2e4d2a62c7d6722686d14e723851"
    balance = events_listener.get_balance(address, 16455322)
    assert float(balance) == 0.204336921515862835

def test_handle_event(events_listener):
    event_data = [{"name": "Transfer", "data": [{"value": "0x30741289523c2e4d2a62c7d6722686d14e723851"}, {"value": "0x30741289523c2e4d2a62c7d6722686d14e723851"}]}]
    events_listener.handle_event({'address': '0xbaac2b4491727d78d2b78815144570b9f2fe8899', 'topics': ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0x00000000000000000000000030741289523c2e4d2a62c7d6722686d14e723851', '0x00000000000000000000000081153f0889ab398c4acb42cb58b565a5392bba95'], 'data': '0x0000000000000000000000000000000000000000000308c2e405267fc05d3400', 'blockNumber': '0xfb3516', 'blockHash': '0x32638c60982ab7b606302fccfc07527e5979fe6e98d6a56575aa6073d6da9c67', 'timeStamp': '0x63cd50cb', 'gasPrice': '0x5823361ac', 'gasUsed': '0xee6b', 'logIndex': '0x1', 'transactionHash': '0x57aa48c5f18b46f3c90c9007f015d22da9dc634046dc965e6802c6a56e364fdd', 'transactionIndex': '0xb'})
    assert True # If the function runs without any errors, the test should pass

def test_insert_user_balance(events_listener):
    mock_db = MagicMock()
    events_listener.db_manager = mock_db
    events_listener.insert_user_balance("0x30741289523c2e4d2a62c7d6722686d14e723851", "0xfc0ad0aa9683b397c1c4da5c11276c630ef7188c9b111086af1298c066305208", datetime.datetime.fromtimestamp(1674488815.6867409), 16455322)
    mock_db.execute.assert_called_once_with(query='INSERT INTO user_balance (balance, address, transaction_hash, transaction_date, total_supply_pct, weekly_change_pct)\n            SELECT %s, %s, %s, %s, %s, %s\n            WHERE NOT EXISTS (\n                SELECT 1 FROM user_balance\n                WHERE address = %s AND transaction_hash = %s);', item_tuple=(Decimal('0.204336921515862835'), '0x30741289523c2e4d2a62c7d6722686d14e723851', '0xfc0ad0aa9683b397c1c4da5c11276c630ef7188c9b111086af1298c066305208', datetime.datetime(2023, 1, 23, 16, 46, 55, 686741), Decimal('1.204128287553646975203096215E-9'), None, '0x30741289523c2e4d2a62c7d6722686d14e723851', '0xfc0ad0aa9683b397c1c4da5c11276c630ef7188c9b111086af1298c066305208'))

def test_calculate_total_supply_pct(events_listener):
    assert events_listener.calculate_total_supply_pct(0) == 0
    assert events_listener.calculate_total_supply_pct(events_listener.total_supply) == 100.0
    assert events_listener.calculate_total_supply_pct(events_listener.total_supply / 2) == 50.0
    assert float(events_listener.calculate_total_supply_pct(events_listener.total_supply / 3)) == 33.333333333333336

def test_calculate_weekly_change(events_listener):
    events_listener.db_manager = MagicMock()

    # insert mock data into the database
    events_listener.db_manager.select_all.return_value = [(1,)]

    result = events_listener.calculate_weekly_change("0x30741289523c2e4d2a62c7d6722686d14e723851", 14, datetime.datetime(2023, 1, 24, 0, 18, 11))
    assert result == 1300.0
    
    events_listener.db_manager.select_all.return_value = [(0.0,)]
    result = events_listener.calculate_weekly_change("0x30741289523c2e4d2a62c7d6722686d14e723851", 0, datetime.datetime(2023, 1, 24, 0, 18, 11))
    assert result == 0.0
    
    events_listener.db_manager.select_all.return_value = [(0,)]
    result = events_listener.calculate_weekly_change("0x30741289523c2e4d2a62c7d6722686d14e723851", 1, datetime.datetime(2023, 1, 24, 0, 18, 11))
    assert result == None
    
    events_listener.db_manager.select_all.return_value = [(1,)]
    result = events_listener.calculate_weekly_change("0x30741289523c2e4d2a62c7d6722686d14e723851", 4, datetime.datetime(2023, 1, 24, 0, 18, 11))
    assert result == 300