import core.constants as const
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from core.utility import logger

class DB:

    engine = None
    connection = None

    def __init__(self) -> None:
        self.connection_string = sqlalchemy.engine.url.URL.create(
            drivername="mysql+pymysql",
            username=const.DB_USER,
            password=const.DB_PASSWORD,
            database=const.DB_NAME,
            host=const.DB_HOST,
        )

    def __del__(self):
        if self.connection is not None:
            self.connection.close()
        if self.engine is not None:
            self.engine.dispose()

    def connect(self):
        try:
            self.engine = sqlalchemy.create_engine(
                self.connection_string,
                pool_size=5,
                max_overflow=2,
                pool_timeout=30,
                pool_recycle=1800,
            )
            self.connection = self.engine.connect()
        except SQLAlchemyError as e:
            logger.error_log(f"DB connection failed: {e}")
            raise RuntimeError("Failed to connect to the database") from e

    def execute(self, query, values=None):
        try:
            res = self.connection.execute(query, values)
            self.connection.commit()
            return (res, None)
        except SQLAlchemyError as e:
            print(f"Error executing query: {e}")
            return (None, e)
