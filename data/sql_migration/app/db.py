from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///../price_training_raw.db')

Base = declarative_base()

class PriceTrainingRaw(Base):
    __tablename__ = 'price_training_zone_label_2024_usd'