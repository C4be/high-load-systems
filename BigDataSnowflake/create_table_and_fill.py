import asyncio
from connection import DBManager
from pathlib import Path
from config import CONN_URL


db = DBManager(db_url=CONN_URL)

# файлы с данными в формате csv
DATA_DIR = Path("./data")
CSV_FILES = list(DATA_DIR.glob("*.csv"))


async def main():
    # Создадим таблички из csv файлов
    for file in CSV_FILES:
        success = await db.create_tables_from_csv(file)
        if success:
            print(f"Таблица из файла {file.name} успешно создана.")
        else:
            print(f"Не удалось создать таблицу из файла {file.name}.")


if __name__ == "__main__":
    asyncio.run(main())