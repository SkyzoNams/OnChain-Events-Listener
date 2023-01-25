# Overview
The purpose of this exercise is to handle Transfer events from the ([The Doge NFT: DOG Token](https://etherscan.io/address/0xbaac2b4491727d78d2b78815144570b9f2fe8899))contract.

A Python program has been implemented in order to index the Transfer events and store the user balance.

There are two way to search for Transfer events on this contract using this program, one is to define two blocks number range to search in,
the other one is to permanently search for new Transfer events on the last mined block.

You can find the documentation for the Practical test [here](https://github.com/SkyzoNams/Data-Engineer-Coding-Challenge/blob/main/documentation.txt) but you will the same information than here.

For this exercise, two version have been made. One [basic](https://github.com/SkyzoNams/Data-Engineer-Coding-Challenge/blob/main/Practical) and a second one [optimized](https://github.com/SkyzoNams/Data-Engineer-Coding-Challenge/blob/main/Practical-optimized).

# Getting Started

1.	Clone the repo
2.  Make sure to have Python 3 installed on your machine (developed with Python 3.7.8)
3.  Go inside the Practical folder from the project root (/Data-Engineer-Coding-Challenge/Practical)
4.  Create your local venv
```bash
python3 -m venv ./venv
```
5.  Activate the venv
```bash
source venv/bin/activate
```
6.	From the project root install all the dependencies
```bash
pip install -r requirements.txt
```

# Usage

You can run the program by executing these commands:

This example will search for Transfer events from the block 13140651 to the latest:
```bash
python blocks_listener.py -from 13140651
```

This example will search for Transfer events from the block 13140651 to the block 13140751:
```bash
python blocks_listener.py -from 13140651 -to 13140751
```

This example will search for Transfer events from the last mined block and continue to the next mined block:
```bash
python blocks_listener.py
```

The program can not be executed with only an -to parameter without a -from

You can deactivate the venv by doing:
```bash
deactivate
```

# How it works [version 1 - basic](https://github.com/SkyzoNams/Data-Engineer-Coding-Challenge/blob/main/Practical)

The Python program will connect to the Infura provider using the Python web3 library in order to get the Ethereum mainnet information, will iterate over blocks regarding two methods 
(from a defined a block number to another block number or continuously to the latest block), then it will create a thread for each block exploration.

A max_threads limit is defined and can be manually adapted to the machine on which the program is executed. If the thread limit is reached,
we wait until a thread to finish his job before starting a new one. Each thread will iterate over the block transaction hashes, extract the receipt from it and
if the receipt concerns our smart contract, try to decode it using the contract abi file. 

If the event has been well decoded, we are able to verify if the event is a Transfer
and store the information in our database.

We request for each user (sender and receiver) their balance in order to store the information. For each record we want to store, we make sure first it has not been stored earlier.

Also, we pre-calculate the share of the total supply owns by the user in percentage and the percentage of changes since 7 days ago.

# How it works [version 2 - optimized](https://github.com/SkyzoNams/Data-Engineer-Coding-Challenge/blob/main/Practical-optimized)

This program will iterate over block numbers to fetch all the events for the token transfer event between `current_block_number` and `last_block_number` using the etherscan api.
Fetching directly the Transfer event using the etherscan api is way more faster than iterate over blocks, transactions and events.

It will handle the 1000 results limit from the etherscan api by incrementing the from_block and to_block values regarding this limit. If the block numbers range is higher than 1000, it will divide the requests regarding the limit.

We request for each user (sender and receiver) their balance in order to store the information. For each record we want to store, we make sure first it has not been stored earlier.

Also, we pre-calculate the share of the total supply owns by the user in percentage and the percentage of changes since 7 days ago.

# Database

To store the information we want, we are using a free AWS RDS database running a PostgresSQL instance.

Two database tables have been created. The user_balance table in order to store user balances regarding their addresses and the transfer_events table in order
to store the event's information.

You can find the tables schemas on the /data/schemas.txt file ([here](https://github.com/SkyzoNams/Data-Engineer-Coding-Challenge/blob/main/Practical/data/schemas.txt)).

The database security is very low for this test case, the data is not encrypted and the database is publicly accessible. 
This decision has been made because the stored data comes from a public source and because this is a test.

The database credentials are stored in the /files/database_params.json ([here](https://github.com/SkyzoNams/Data-Engineer-Coding-Challenge/blob/main/Practical/files/database_params.json)). That's definitely not secured at all, but it will only be used on test mode.

We use the python psycopg2 library to access the database.

# Engineering team

In order to let the data consumed by the engineering team, we have created the Engineering class [here](https://github.com/SkyzoNams/Data-Engineer-Coding-Challenge/blob/main/Practical/engineering.py).

This class contains built-in methods:

    get_holders: returns token holder's information with the pre-calculated weekly change percentage and the total supply percentage.
    You can add a limit to your request by passing an integer value as parameter, by default there is no limit for this result.
    
    get_top_100_holders: returns top 100 holders information with the pre-calculated weekly change percentage and the total supply percentage.
    You can add a limit to your request by passing an integer value as parameter and increase or reduce the holder classification, by default there is a 100 results limit.

    decode_records: methods will create an array of dict with human digestible key names.

# Testing

A complete testing suits have been implemented on the /tests/test_events_listener.py ([here](https://github.com/SkyzoNams/Data-Engineer-Coding-Challenge/blob/main/Practical/tests/test_events_listener.py)) file using pytest.

You can run all the tests by executing "pytest" on the root of the Practical folder. Please make sure the venv has been activated first.
```bash
pytest
```

# Improvements

- Reducing the database accessibility and encrypt the stored data.
- Store the database credentials in a key vault.

- We are using a free Infura provider plan that have requesting limits. Increasing the daily limit will be necessary on production. 
    - A different implementation could reduce the number of Infura api call.

- For the optimized version, we are using a free etherscan api plan that have requesting limits, 5 per seconds or 100,000 call per day. Increasing the daily limit could be necessary on production. 

- We are also using a free db.t3.micro AWS RDS database, on production a database with more storage and better capacities will be necessary.
    - In order to make scalable to 100 million entries there is two options:
        - Horizontally scaling the database: You can add more RDS instances to handle the increased load, and use a load balancer to distribute the incoming data across the instances.
        - Optimizing the database: You can optimize the database's performance by indexing the relevant columns and fine-tuning the configuration settings.

- Going deeper on tests.
- Adding engineering methods.
- Find a way to kill the thread as soon as he finished its last task for the version 1.

# Performance

1. version 1
On my local machine with 10 CPU cores and 16 GPU cores it would take 11091 hours so 462 days, so almost 1 year and 2 months if you want the program to start processing all the blocks since the contract creation (block #13140651)

The process execution could be distributed simultaneously to different machine, each one processing a batch of blocks in order to save some precious time.

However, the program always handles new mined blocks one after the other.

2. version 2 optimized
The optimized version, using the etherscan api is way more faster and has less iterations.
It tooks 1995 seconds so approximately 33.3 minutes on my local machine to process 10 000 blocks (from block 13140651 to block 13150651) but it will vary depending on the number of Transfer events in this block range.