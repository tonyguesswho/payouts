import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from statistics import mean
from typing import Dict, List, Optional

import aiohttp
from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from websockets.exceptions import ConnectionClosedOK

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CONTRACT_ADDRESS = "0x1B34bCc581d535D33C895fABce3c85F1bF3bdb33"
ARBISCAN_API_URL = "https://api.arbiscan.io/api"
API_KEY = os.environ.get("ARBISCAN_API_KEY", "")
API_UPDATE_INTERVAL = 5 * 60 * 60  # 5 hours in seconds
WEBHOOK_UPDATE_INTERVAL = 6 * 60 * 60  # 6 hours in seconds

FIRMS = [
    {"name": "GFT", "logo": "gft_logo_url", "wallet": "0xD3E70282420E6349bA3146bc299c4943f9782667"},
    # Add more firms here...
]

# Cache to store all transactions (24 hours TTL)
cache = TTLCache(maxsize=100, ttl=24*60*60)

class Payout(BaseModel):
    value: float
    timestamp: datetime

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class FirmSummary(BaseModel):
    name: str
    logo: str
    total_payouts: float
    num_payouts: int
    largest_single_payout: float
    average_payout_size: float
    all_payouts: List[Payout]
    top_10_payouts: List[Payout]
    time_since_last_payout: Optional[str]

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

async def get_token_transactions_batch(wallet_addresses: List[str]) -> Dict[str, List[Dict]]:
    print("Getting transaction from api")
    addresses = ",".join(wallet_addresses)
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": CONTRACT_ADDRESS,
        "address": addresses,
        "sort": "desc",
        "apikey": API_KEY
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(ARBISCAN_API_URL, params=params) as response:
            data = await response.json()
            if data["status"] != "1":
                raise HTTPException(status_code=400, detail=f"API Error: {data['message']}")

            transactions_by_wallet = {addr.lower(): [] for addr in wallet_addresses}
            for tx in data["result"]:
                if tx["from"].lower() in transactions_by_wallet:
                    transactions_by_wallet[tx["from"].lower()].append(tx)
                elif tx["to"].lower() in transactions_by_wallet:
                    transactions_by_wallet[tx["to"].lower()].append(tx)

            return transactions_by_wallet

def calculate_time_since_last_payout(last_payout_timestamp: datetime) -> str:
    time_difference = datetime.now() - last_payout_timestamp
    days = time_difference.days
    hours, remainder = divmod(time_difference.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    if days > 0:
        return f"{days} days, {hours} hours, {minutes} minutes"
    elif hours > 0:
        return f"{hours} hours, {minutes} minutes"
    else:
        return f"{minutes} minutes"

def process_transactions(transactions: List[Dict], wallet_address: str) -> FirmSummary:
    outgoing_txs = [
        Payout(
            value=float(tx["value"]) / (10 ** int(tx["tokenDecimal"])),
            timestamp=datetime.fromtimestamp(int(tx["timeStamp"]))
        )
        for tx in transactions
        if tx["from"].lower() == wallet_address.lower()
    ]

    if not outgoing_txs:
        return None

    total_payouts = sum(tx.value for tx in outgoing_txs)
    num_payouts = len(outgoing_txs)
    largest_single_payout = max(tx.value for tx in outgoing_txs)
    average_payout_size = mean(tx.value for tx in outgoing_txs) if outgoing_txs else 0
    top_10_payouts = sorted(outgoing_txs, key=lambda x: x.value, reverse=True)[:10]

    last_payout_timestamp = max(tx.timestamp for tx in outgoing_txs)
    time_since_last_payout = calculate_time_since_last_payout(last_payout_timestamp)

    return FirmSummary(
        name=next(firm["name"] for firm in FIRMS if firm["wallet"].lower() == wallet_address.lower()),
        logo=next(firm["logo"] for firm in FIRMS if firm["wallet"].lower() == wallet_address.lower()),
        total_payouts=total_payouts,
        num_payouts=num_payouts,
        largest_single_payout=largest_single_payout,
        average_payout_size=average_payout_size,
        all_payouts=outgoing_txs,
        top_10_payouts=top_10_payouts,
        time_since_last_payout=time_since_last_payout
    )

async def update_cache():
    while True:
        wallet_addresses = [firm["wallet"].lower() for firm in FIRMS]
        try:
            transactions_by_wallet = await get_token_transactions_batch(wallet_addresses)
            for wallet, transactions in transactions_by_wallet.items():
                cache[wallet] = transactions
        except Exception as e:
            logger.error(f"Error updating cache: {e}")
        await asyncio.sleep(API_UPDATE_INTERVAL)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_cache())

@app.get("/firms", response_model=List[FirmSummary])
async def get_firms_summary():
    summaries = []
    wallet_addresses = [firm["wallet"].lower() for firm in FIRMS]

    for firm in FIRMS:
        wallet = firm["wallet"].lower()
        transactions = cache.get(wallet, [])

        if not transactions:
            # If not in cache, fetch for this specific wallet
            transactions_by_wallet = await get_token_transactions_batch([wallet])
            transactions = transactions_by_wallet.get(wallet, [])
            cache[wallet] = transactions

        summary = process_transactions(transactions, wallet)
        if summary:
            summaries.append(summary)

    return summaries

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Payout):
            return obj.dict()
        elif isinstance(obj, FirmSummary):
            return obj.dict()
        return super().default(obj)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    logger.info("WebSocket connection attempt")
    try:
        await websocket.accept()
        logger.info("WebSocket connection accepted")
        try:
            while True:
                summaries = []
                for firm in FIRMS:
                    wallet = firm["wallet"].lower()
                    transactions = cache.get(wallet, [])
                    summary = process_transactions(transactions, wallet)
                    if summary:
                        summaries.append(summary)

                # Use custom JSON encoder to handle datetime serialization
                json_compatible_summaries = json.dumps(summaries, cls=CustomJSONEncoder)
                await websocket.send_text(json_compatible_summaries)

                logger.info("Data sent through WebSocket")
                await asyncio.sleep(WEBHOOK_UPDATE_INTERVAL)
        except WebSocketDisconnect:
            logger.info("WebSocket disconnected")
        except ConnectionClosedOK:
            logger.info("WebSocket connection closed normally")
        except Exception as e:
            logger.error(f"Error in WebSocket communication: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to establish WebSocket connection: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")