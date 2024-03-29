from sqlalchemy import create_engine, Column, Integer, String, select, CursorResult
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Create an engine for the PostgreSQL database
engine = create_engine(
    "postgresql+pg8000://postgres:1234@localhost/upbitDB",
    isolation_level="REPEATABLE READ",
    client_encoding='utf8',
    echo=True
)

# Create a base class for declarative class definitions
Base = declarative_base()


# Define a User class representing the users table
class CodeAlarm(Base):
    __tablename__ = 'CODE_ALARM'

    id = Column(String, primary_key=True)
    title = Column(String, nullable=False)
    thread_id = Column(String, nullable=True)
    discord_webhook_url = Column(String, nullable=True)

    # use for clean express
    def __repr__(self):
        return (f"CodeAlarm(id={self.id}, title={self.title},"
                f" thread_id={self.thread_id}, discord_webhook_url={self.discord_webhook_url}")

    @classmethod
    def create(cls, _id: str, title: str, thread_id: str, discord_webhook_url: str) -> "CodeAlarm":
        return cls(
            id=_id,
            title=title,
            thread_id=thread_id,
            discord_webhook_url=discord_webhook_url,
        )


# Create the users table in the database
Base.metadata.create_all(engine)

# Create a sessionmaker bound to the engine
Session = sessionmaker(bind=engine)
session = Session()


# Read operation
def get_by_id(_id) -> CodeAlarm | None:
    return session.execute(select(CodeAlarm).where(CodeAlarm.id == _id)).scalar()
