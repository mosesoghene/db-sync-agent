import pymysql

def connect_mysql(db_config):
    """Create a pymysql connection using a DB config dict."""
    return pymysql.connect(
        host=db_config["host"],
        port=db_config.get("port", 3306),
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["db"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )