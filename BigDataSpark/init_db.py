# ===========================================
# Configs
# ===========================================

DB_USER = "bober"
DB_PASS = "bober_kurwa"
DB_HOST = "localhost"
DB_PORT = "5432" 
DB_NAME = "bober_db"
CONN_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
ECHO = False