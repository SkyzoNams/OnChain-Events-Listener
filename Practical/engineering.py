
from src.database_manager import DataBaseManager
from src.events_handler import Events_Listener

class Engineering():
    def __init__(self):
        pass
    
    def get_holders(self, limit=None):
        query = """SELECT DISTINCT address, id, balance, transaction_hash, created_at, transaction_date
            FROM user_balance
            ORDER BY transaction_date DESC"""
        if limit is not None:
            query +=  " LIMIT " + str(limit)
        return DataBaseManager().select_all(query=query)

    def get_top_100_holders(self):
        total_supply = Events_Listener().get_total_supply()
        total_supply = Events_Listener().provider().fromWei(total_supply, 'ether')
        query = """SELECT address, balance, (balance / """ + str(total_supply) + """) * 100 as "percent_of_total_supply"
            FROM (SELECT DISTINCT ON (address) address, balance, transaction_date FROM user_balance ORDER BY address, transaction_date DESC) as last_transaction
            ORDER BY balance DESC
            LIMIT 100;"""
        return DataBaseManager().select_all(query=query)

    def get_holders_weekly_change(self, limit=None):
        query = """WITH weekly_change AS (
            SELECT address, balance,
                balance - lag(balance) over (partition by address order by transaction_date) as weekly_change
            FROM user_balance
        )
        SELECT *, weekly_change FROM weekly_change"""
        if limit is not None:
            query +=  " LIMIT " + str(limit)
        return DataBaseManager().select_all(query=query)



    
if __name__ == "__main__":
    print(Engineering().get_top_100_holders())