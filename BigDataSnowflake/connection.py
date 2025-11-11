import pandas as pd
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    AsyncEngine,
    async_sessionmaker,
)
from typing import AsyncGenerator, Union
from pathlib import Path


class DBManager:
    def __init__(self, db_url: str, pool_size: int = 5, max_overflow: int = 10, echo: bool = True):
        self.db_url: str = db_url
        try:
            self.engine: AsyncEngine = create_async_engine(
                self.db_url,
                pool_size=pool_size,
                max_overflow=max_overflow,
                echo=echo,
                pool_recycle=3600,
            )
            
            self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
                bind=self.engine,
                expire_on_commit=False,  # important for async sessions
            )
            
        except ImportError as ie:
            raise ImportError(f"Ошибка драйвера: {ie}")
        except Exception as e:
            raise RuntimeError(f"Ошибка движка: {e}")
    
    
    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        session: AsyncSession = self._session_factory()
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            raise RuntimeError(f"Ошибка запроса к базе данных: {e}")
        finally:
            await session.close()

    
    async def create_tables_from_csv(self, file_csv: Union[Path, str]) -> bool:
        try:
            if isinstance(file_csv, str):
                file_csv = Path(file_csv)

            df = pd.read_csv(file_csv)
            # table_name = file_csv.stem
            table_name = "mock_data"

            # Используем напрямую async engine (а не session)
            async with self.engine.begin() as conn:
                await conn.run_sync(
                    lambda sync_conn: df.to_sql(
                        name=table_name,
                        con=sync_conn,
                        if_exists='append',
                        index=False
                    )
                )

            print(f"✅ Таблица '{table_name}' успешно загружена.")
            return True

        except Exception as e:
            print(f"❌ Критическая ошибка при загрузке CSV: {e}")
            print(f"Не удалось создать таблицу из файла {file_csv.name}.")
            return False
