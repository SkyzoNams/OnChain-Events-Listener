
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
        """
        calculates the percentage change compared to 7 days ago
        """
        query = """WITH latest_transactions AS (
            SELECT address, MAX(transaction_date) as latest_transaction_date, MAX(balance) as balance
            FROM user_balance
            GROUP BY address
            )
            SELECT 
                t1.address,
                t1.balance, 
                t1.transaction_hash,
                t1.created_at,
                t1.transaction_date,
                CASE 
                    WHEN t1.balance = 0 THEN 0 
                    ELSE ((t2.balance - t1.balance) / t1.balance) * 100 
                END AS weekly_change_pct
            FROM user_balance t1
            JOIN latest_transactions t2 ON t1.address = t2.address AND t1.transaction_date = t2.latest_transaction_date
            ORDER BY t1.transaction_date DESC"""
        if limit is not None:
            query +=  " LIMIT " + str(limit)
        return DataBaseManager().select_all(query=query)


    def get_holders_change_compare_to_last_week(self, limit=None):
        """
        calculates the percentage change during the last 7 days
        """
        query = """WITH latest_transactions AS (
            SELECT address, MAX(transaction_date) as latest_transaction_date, MAX(balance) as balance
            FROM user_balance
            WHERE transaction_date >= (now() - interval '7 days')
            GROUP BY address
            )
            SELECT 
                t1.address,
                t1.balance, 
                t1.transaction_hash,
                t1.created_at,
                t1.transaction_date,
                CASE 
                    WHEN t1.balance = 0 THEN 0 
                    ELSE ((t2.balance - t1.balance) / t1.balance) * 100 
                END AS weekly_change_pct
            FROM user_balance t1
            JOIN latest_transactions t2 ON t1.address = t2.address AND t1.transaction_date = t2.latest_transaction_date
            ORDER BY t1.transaction_date DESC"""
        if limit is not None:
            query +=  " LIMIT " + str(limit)
        return DataBaseManager().select_all(query=query)

    
if __name__ == "__main__":
    print(Engineering().get_holders_change_compare_to_last_week(10))