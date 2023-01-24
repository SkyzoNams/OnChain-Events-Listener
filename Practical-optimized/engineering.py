
from src.database_manager import DataBaseManager

class Engineering():
    def __init__(self):
        pass
    
    def get_holders(self, limit=None):
        """
        returns the holders balance starting from the last transaction. You can add a limit in the results.
        """
        query = """SELECT DISTINCT address, id, balance, transaction_hash, created_at, transaction_date, total_supply_pct, weekly_change_pct
            FROM user_balance ORDER BY transaction_date DESC"""
        if limit is not None:
            query +=  " LIMIT " + str(limit)
        return DataBaseManager().select_all(query=query)

    def get_top_100_holders(self, limit=100):
        """
        returns the top 100 holders records with the percentage of the total supply they own
        """
        query = """SELECT * FROM (SELECT DISTINCT ON (address) address, id, balance, transaction_hash, created_at, transaction_date, total_supply_pct, weekly_change_pct FROM user_balance ORDER BY address, transaction_date DESC) 
            as last_transaction ORDER BY balance DESC LIMIT """
        query += str(limit)
        return DataBaseManager().select_all(query=query)
        
def decode_records(records):
    """
    create an array of dict with human digestible key names
    """
    result = []
    for record in records:
        if len(record) >= 8:
            result.append({
                "user_address": record[0],
                "id": record[1],
                "balance": record[2],
                "transaction_hash": record[3],
                "created_at": record[4],
                "transaction_date": record[5],
                "total_supply_pct": record[6],
                "weekly_change_pct": record[7]
            })
    return result
    
if __name__ == "__main__":
    print(decode_records(Engineering().get_holders(10)))
    print('X' * 50)
    print(decode_records(Engineering().get_top_100_holders()))