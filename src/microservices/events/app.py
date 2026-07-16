import asyncio
import json
import uuid
import os
from datetime import datetime
from typing import Dict, Any, Optional, Union
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
import uvicorn

from kafka_config import kafka_config

# Pydantic models with more flexible validation
class EventResponse(BaseModel):
    status: str
    event_id: str
    message: str
    timestamp: str


class MovieEvent(BaseModel):
    movie_id: Union[int, str] = Field(..., description="Movie ID")
    title: str
    genre: Optional[str] = None
    year: Optional[Union[int, str]] = None
    action: str = Field(..., description="CREATE, UPDATE, DELETE")
    user_id: Optional[Union[int, str]] = None
    
    @validator('movie_id')
    def validate_movie_id(cls, v):
        if isinstance(v, str) and v.isdigit():
            return int(v)
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "movie_id": 123,
                "title": "Inception",
                "genre": "Sci-Fi",
                "year": 2010,
                "action": "CREATE"
            }
        }


class UserEvent(BaseModel):
    user_id: Union[int, str] = Field(..., description="User ID")
    username: str
    email: str
    action: str = Field(..., description="REGISTER, UPDATE, DELETE")
    
    @validator('user_id')
    def validate_user_id(cls, v):
        if isinstance(v, str) and v.isdigit():
            return int(v)
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": 1,
                "username": "johndoe",
                "email": "john@example.com",
                "action": "REGISTER"
            }
        }


class PaymentEvent(BaseModel):
    payment_id: Union[int, str] = Field(..., description="Payment ID")
    user_id: Union[int, str] = Field(..., description="User ID")
    amount: Union[float, str] = Field(..., description="Amount")
    currency: str = "USD"
    status: str = Field(..., description="PENDING, COMPLETED, FAILED")
    
    @validator('payment_id')
    def validate_payment_id(cls, v):
        if isinstance(v, str) and v.isdigit():
            return int(v)
        return v
    
    @validator('user_id')
    def validate_user_id(cls, v):
        if isinstance(v, str) and v.isdigit():
            return int(v)
        return v
    
    @validator('amount')
    def validate_amount(cls, v):
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                pass
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "payment_id": 1,
                "user_id": 1,
                "amount": 19.99,
                "currency": "USD",
                "status": "COMPLETED"
            }
        }


# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Starting Events Microservice...")
    print(f"📡 Service will listen on: 0.0.0.0:{os.getenv('PORT', 8082)}")
    
    # Initialize Kafka producer
    producer_started = await kafka_config.init_producer()
    if not producer_started:
        print("⚠️  Kafka producer failed to start")
    else:
        print("✅ Kafka producer started successfully")
    
    # Initialize Kafka consumer for all topics
    consumer_started = await kafka_config.init_consumer(
        ["movie-events", "user-events", "payment-events"]
    )
    if not consumer_started:
        print("⚠️  Kafka consumer failed to start")
    else:
        print("✅ Kafka consumer started successfully")
    
    # Start consumer in background
    if consumer_started:
        asyncio.create_task(kafka_config.consume_events(
            ["movie-events", "user-events", "payment-events"]
        ))
        print("📨 Kafka consumer started in background")
    
    print("✅ Application startup complete!")
    print(f"🌐 Health check available at: http://localhost:8082/health")
    print(f"🌐 Events API available at: http://localhost:8082/api/events")
    
    yield
    
    # Shutdown
    print("🔄 Shutting down Events Microservice...")
    await kafka_config.close()


app = FastAPI(
    title="Events Microservice",
    description="Event handling service with Kafka integration",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "events-service",
        "kafka": "connected" if kafka_config.producer else "disconnected"
    }


@app.post("/api/events/movie")
async def create_movie_event(request: Request):
    """Create a movie event and publish to Kafka - flexible endpoint"""
    try:
        # Get raw body
        body = await request.json()
        
        # Handle different input formats
        event_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        
        # Extract data with defaults
        movie_data = {
            "movie_id": body.get("movie_id", body.get("movieId", body.get("id", 0))),
            "title": body.get("title", "Unknown"),
            "genre": body.get("genre", "Unknown"),
            "year": body.get("year", 0),
            "action": body.get("action", "CREATE"),
            "user_id": body.get("user_id", body.get("userId", None))
        }
        
        event_data = {
            "event_id": event_id,
            "event_type": "movie",
            "timestamp": timestamp,
            "data": movie_data
        }
        
        # Publish event to Kafka
        success = await kafka_config.produce_event("movie-events", event_data)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to publish event to Kafka")
        
        return JSONResponse(
            status_code=201,
            content=EventResponse(
                status="success",
                event_id=event_id,
                message="Movie event published successfully",
                timestamp=timestamp
            ).dict()
        )
    except Exception as e:
        print(f"Error in movie event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/user")
async def create_user_event(request: Request):
    """Create a user event and publish to Kafka - flexible endpoint"""
    try:
        # Get raw body
        body = await request.json()
        
        event_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        
        # Extract data with defaults
        user_data = {
            "user_id": body.get("user_id", body.get("userId", body.get("id", 0))),
            "username": body.get("username", body.get("name", "Unknown")),
            "email": body.get("email", "unknown@example.com"),
            "action": body.get("action", "REGISTER")
        }
        
        event_data = {
            "event_id": event_id,
            "event_type": "user",
            "timestamp": timestamp,
            "data": user_data
        }
        
        # Publish event to Kafka
        success = await kafka_config.produce_event("user-events", event_data)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to publish event to Kafka")
        
        return JSONResponse(
            status_code=201,
            content=EventResponse(
                status="success",
                event_id=event_id,
                message="User event published successfully",
                timestamp=timestamp
            ).dict()
        )
    except Exception as e:
        print(f"Error in user event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/payment")
async def create_payment_event(request: Request):
    """Create a payment event and publish to Kafka - flexible endpoint"""
    try:
        # Get raw body
        body = await request.json()
        
        event_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        
        # Extract data with defaults
        payment_data = {
            "payment_id": body.get("payment_id", body.get("paymentId", body.get("id", 0))),
            "user_id": body.get("user_id", body.get("userId", 0)),
            "amount": float(body.get("amount", 0.0)),
            "currency": body.get("currency", "USD"),
            "status": body.get("status", "COMPLETED")
        }
        
        event_data = {
            "event_id": event_id,
            "event_type": "payment",
            "timestamp": timestamp,
            "data": payment_data
        }
        
        # Publish event to Kafka
        success = await kafka_config.produce_event("payment-events", event_data)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to publish event to Kafka")
        
        return JSONResponse(
            status_code=201,
            content=EventResponse(
                status="success",
                event_id=event_id,
                message="Payment event published successfully",
                timestamp=timestamp
            ).dict()
        )
    except Exception as e:
        print(f"Error in payment event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/events/health")
async def events_health():
    """Health check endpoint for API tests"""
    return {
        "status": True,
        "service": "events-service",
        "version": "1.0.0",
        "kafka_connected": bool(kafka_config.producer),
        "port": os.getenv("PORT", 8082)
    }


@app.get("/api/events/test")
async def test_kafka():
    """Test endpoint to verify Kafka integration"""
    test_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "test",
        "timestamp": datetime.utcnow().isoformat(),
        "data": {"message": "Test Kafka connection"}
    }
    
    success = await kafka_config.produce_event("movie-events", test_event)
    
    return {
        "status": "success" if success else "failed",
        "message": "Test event published" if success else "Failed to publish test event"
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for better error messages"""
    print(f"Global error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"status": "error", "message": str(exc)}
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8082))
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info"
    )