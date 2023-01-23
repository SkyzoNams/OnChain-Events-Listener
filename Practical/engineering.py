
from src.database_manager import DataBaseManager
from src.events_handler import Events_Listener

class Engineering():
    def __init__(self):
        pass
    
    def get_holders(self, limit=None):
        query = """WITH last_created AS (
            SELECT address, max(transaction_date) as last_created
            FROM user_balance
            GROUP BY address
        )
        SELECT user_balance.*
        FROM user_balance
        JOIN last_created
        ON user_balance.address = last_created.address
        AND user_balance.transaction_date = last_created.last_created"""
        if limit is not None:
            query +=  " LIMIT " + str(limit)
        return DataBaseManager().select_all(query=query)

    def get_top_100_holders(self):
        total_supply = Events_Listener().get_total_supply()
        total_supply = Events_Listener().provider().fromWei(total_supply, 'ether')
        query = """WITH last_created AS (
                SELECT address, max(transaction_date) as last_created
                FROM user_balance
                GROUP BY address
            ), total_supply AS (
                SELECT """ + str(total_supply) + """ AS total_supply
            )
            SELECT user_balance.*, (user_balance.balance::numeric / total_supply.total_supply) * 100 as percentage_of_total_supply
            FROM user_balance
            JOIN last_created
            ON user_balance.address = last_created.address
            AND user_balance.transaction_date = last_created.last_created
            JOIN total_supply
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
    print(Engineering().get_holders_weekly_change(20))