from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class MessageBase(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    message_length: int
    view_count: int
    forward_count: int
    has_image: bool


class Message(MessageBase):
    id: int
    message_timestamp: datetime

    class Config:
        from_attributes = True


class ChannelBase(BaseModel):
    channel_name: str
    channel_type: str
    total_posts: int
    avg_views: float


class Channel(ChannelBase):
    channel_key: str

    class Config:
        from_attributes = True

class TopProduct(BaseModel):
    product: str
    mention_count: int

class ChannelActivity(BaseModel):
    date: datetime
    message_count: int

class VisualContentStats(BaseModel):
    channel_name: str
    total_images: int
    avg_confidence: float
    promotional_count: int
    product_display_count: int