from sqlalchemy import create_engine
import urllib.parse

def get_engine():
    user = "postgres"
    password = "" # Put your password here
    # This line handles special characters like @, !, # in your password
    safe_password = urllib.parse.quote_plus(password)
    
    host = "localhost"
    port = "5432"
    db_name = "finance_db"
    
    url = f"postgresql://{user}:{safe_password}@{host}:{port}/{db_name}"
    engine = create_engine(url)
    return engine