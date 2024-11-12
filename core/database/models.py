from sqlalchemy import Column, Integer, String, DateTime, JSON, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class UpdateHistory(Base):
    __tablename__ = "update_history"

    id = Column(Integer, primary_key=True, index=True)
    dataset_name = Column(String, index=True)
    update_type = Column(String)  # 'socrata', 'sms', 'ftp'
    status = Column(String)  # 'success', 'failed'
    timestamp = Column(DateTime, default=datetime.utcnow)
    details = Column(JSON)

class WebhookSubscription(Base):
    __tablename__ = "webhook_subscriptions"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(String)
    event_type = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_triggered = Column(DateTime, nullable=True) 