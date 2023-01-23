
from src.database_manager import DataBaseManager

class Engineering():
    def __init__(self):
        pass
    
    def get_holders(self, limit=None):
        query = """WITH last_created AS (
            SELECT address, max(created_at) as last_created
            FROM user_balance
            GROUP BY address
        )
        SELECT user_balance.*
        FROM user_balance
        JOIN last_created
        ON user_balance.address = last_created.address
        AND user_balance.created_at = last_created.last_created"""
        if limit is not None:
            query +=  " LIMIT " + str(limit)

    def get_top_100_holders(self):
        query = "SELECT * FROM user_balance ORDER BY balance ASC LIMIT 100"
    
    def get_holders_weekly_change(self, limit=None):
        query = """WITH weekly_change AS (
            SELECT address,
                balance - lag(balance) over (partition by address order by created_at) as weekly_change
            FROM user_balance
        )
        SELECT *, weekly_change
        FROM weekly_change"""
        if limit is not None:
            query +=  " LIMIT " + str(limit)
    