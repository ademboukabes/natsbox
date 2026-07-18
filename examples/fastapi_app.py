# pyright: reportMissingImports=false
"""
FastAPI Example with nats-outbox.

Run this example:
1. Ensure Postgres and NATS are running locally.
2. OUTBOX_DATABASE_URL="postgresql+asyncpg://outbox:outbox@localhost:5432/outbox_test" \
   uvicorn examples.fastapi_app:app --reload
3. In another terminal, start the relay:
   OUTBOX_DATABASE_URL="..." nats-outbox relay start
"""

import os
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# ── 1. Domain Setup ─────────────────────────────────────────────────────────
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from nats_outbox.core.models import create_tables
from nats_outbox.core.outbox import outbox_transaction


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid.uuid4()))
    email: Mapped[str] = mapped_column(unique=True)
    name: Mapped[str] = mapped_column()


# ── 2. Database Setup ───────────────────────────────────────────────────────

# Update this URL to match your local PostgreSQL instance
DATABASE_URL = os.getenv(
    "OUTBOX_DATABASE_URL", "postgresql+asyncpg://outbox:outbox@localhost:5432/outbox_test"
)
engine = create_async_engine(DATABASE_URL)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Create domain tables and outbox tables on startup (for demo purposes)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await create_tables(engine)
    yield
    await engine.dispose()


app = FastAPI(lifespan=lifespan)


# Dependency to get DB session
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session


# ── 3. API Endpoints ────────────────────────────────────────────────────────


class CreateUserRequest(BaseModel):
    email: str
    name: str


class UserResponse(BaseModel):
    id: str
    email: str
    name: str


@app.post("/users", response_model=UserResponse, status_code=201)
async def create_user(
    req: CreateUserRequest, session: AsyncSession = Depends(get_session)
) -> UserResponse:
    """
    Creates a user in the database AND schedules an outbox event atomically.
    """
    user_id = str(uuid.uuid4())
    user = User(id=user_id, email=req.email, name=req.name)

    try:
        async with outbox_transaction(session) as tx:
            # 1. Mutate domain state
            session.add(user)

            # 2. Schedule outbox event in the same transaction
            tx.publish_event(
                subject="user.created",
                payload={"id": user_id, "email": req.email, "name": req.name},
                aggregate_id=user_id,
                aggregate_type="user",
            )
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail="Email already registered") from e
    return UserResponse(id=user_id, email=req.email, name=req.name)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
