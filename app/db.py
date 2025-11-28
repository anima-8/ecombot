from motor.motor_asyncio import AsyncIOMotorClient
from app.config import get_settings

settings = get_settings()

client = AsyncIOMotorClient(settings.MONGODB_URI)
db = client["ecom"]

users_collection = db["users"]
orders_collection = db["orders"]
calcs_collection = db["calcs"]
