import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from statistics import mean
from typing import Dict, List, Optional

import aiohttp
from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from websockets.exceptions import ConnectionClosedOK

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
CONTRACT_ADDRESS = "0x1B34bCc581d535D33C895fABce3c85F1bF3bdb33"
ARBISCAN_API_URL = "https://api.arbiscan.io/api"
API_KEY = os.environ.get("ARBISCAN_API_KEY", "")
API_UPDATE_INTERVAL = 5 * 60 * 60  # 5 hours in seconds
WEBHOOK_UPDATE_INTERVAL = 6 * 60 * 60  # 6 hours in seconds

FIRMS = [
    {
        "name": "Goat Funded Trader",
        "id": "66ca580dffaeae770f1498d8",
        "wallet": "0xD3E70282420E6349bA3146bc299c4943f9782667",
    },
    {
        "name": "Funding pips",
        "id": "66cb30fa8c725024fcd80ef2",
        "wallet": "0x1e198Ad0608476EfA952De1cD8e574dB68df5f16",
    },
    {
        "name": "Alpha capital",
        "id": "66cb31048376632dcc6a2a66",
        "wallet": "0xD172B9C227361FCf6151e802e1F09C084964BDCD",
    },
    {
        "name": "Funded Peaks",
        "id": "66cb310d796c0ab0a80632a3",
        "wallet": "0xd0D96d8Ad9c5f92b66A3b0d721c70D31da582C38",
    },
    {
        "name": "MYFUNDEDFX",
        "id": "66cb6fb78376632dcc9fa7a3",
        "wallet": "0x5534A2237f866F3089901A7D17f0E50dD7824c8D",
    },
    {
        "name": "E8 markets",
        "id": "66cb6fc8d7c63944198a4e21",
        "wallet": "0xD19e945EEea57475B791C20112Ebb4B628f5B95c",
    },
    {
        "name": "The 5ers",
        "id": "66cb6ff62a689130fc027367",
        "wallet": "0x349B0Ed1520eAE1472f57eaC77e390A1eCB0C677",
    },
    {
        "name": "Pip farm",
        "id": "66cb7008ecc3e28071dc267c",
        "wallet": "0x442775D9FcbcD269bbdB7677ebd7f7D34dA5569F",
    },
    {
        "name": "My flash funding",
        "id": "66cb70231238deea1b8e0dd3",
        "wallet": "0xeCa4DcC2D6B0ab4A496A3bDEC3cA07f95022cBdB",
    },
    {
        "name": "Instant funding",
        "id": "66cb7033c6fa014536c41546",
        "wallet": "0xA3f21b162fA5a523e12590f2915cA418587Cf626",
    },
    {
        "name": "Blue guardian",
        "id": "66cb70555230d82035fff546",
        "wallet": "0x68035843020B6c0CD94DD29c273FFF13c8e9A914",
    },
    {
        "name": "ascend X",
        "id": "66cb6fa0ffd1354e499a8b2c",
        "wallet": "0x858fcd36e5a8aBD64b955b1E2a70F3F75A464AFd",
    },
    {
        "name": "FXIFY",
        "id": "66cb83bdedabdad8909e66f5",
        "wallet": "0x36109F4D6804f391D830939c0C0e43EFc41a7486",
    },
    {
        "name": "Funding Traders",
        "id": "66cba1ffecc3e2807105cd15",
        "wallet": "0xe497F35cd0b928b686e9fb2e3cD131949D1841Cf",
    },
    {
        "name": "Funded Next",
        "id": "66cc776ec15ebbd49e0b8f0c",
        "wallet": "0x2b9a16e8448091159cc2b2a205b11f2368d53cb6",
    },
    {
        "name": "Artic Funding",
        "id": "66cf2f4777b50737924ac87e",
        "wallet": "0xD4b7E42c1E0C0fDb4285d1dcfCe253C221a30b39",
    },
    {
        "name": "TopTierTrader",
        "id": "66d26a7e7fa8b1bcae4c4bbc",
        "wallet": "0x2265f428a8803bcc6Ccf4A405E627B0c51f33389",
    },
]

# Cache to store processed FirmSummary objects (24 hours TTL)
cache = TTLCache(maxsize=100, ttl=24 * 60 * 60)


class Payout(BaseModel):
    value: float
    timestamp: datetime

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class TimeRangeSummary(BaseModel):
    total_payouts: float
    num_payouts: int
    largest_single_payout: float
    average_payout_size: float


class FirmSummary(BaseModel):
    name: str
    id: str
    last_24h: TimeRangeSummary
    last_7d: TimeRangeSummary
    last_30d: TimeRangeSummary
    all_time: TimeRangeSummary
    last_10_payouts: List[Payout]
    top_10_largest_payouts: List[Payout]
    time_since_last_payout: Optional[str]

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class RateLimiter:
    def __init__(self, calls: int, period: float):
        self.calls = calls
        self.period = period
        self.timestamps = []

    async def wait(self):
        now = time.time()
        self.timestamps = [t for t in self.timestamps if now - t < self.period]

        if len(self.timestamps) >= self.calls:
            sleep_time = self.period - (now - self.timestamps[0])
            if sleep_time > 0:
                logger.debug("Rate limit reached. Waiting for %.2f seconds", sleep_time)
                await asyncio.sleep(sleep_time)

        self.timestamps.append(time.time())


rate_limiter = RateLimiter(calls=1, period=4.0)  # 5 calls per second


async def get_token_transactions(wallet_address: str) -> List[Dict]:
    await rate_limiter.wait()
    logger.info("Fetching transactions for wallet: %s", wallet_address)
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": CONTRACT_ADDRESS,
        "address": wallet_address,
        "sort": "desc",
        "apikey": API_KEY,
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(ARBISCAN_API_URL, params=params) as response:
            data = await response.json()
            if data["status"] != "1":
                logger.error("API Error for %s: %s", wallet_address, data)
                return []
            logger.info(
                "Successfully fetched %d transactions for %s",
                len(data["result"]),
                wallet_address,
            )
            return data["result"]


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


def summarize_transactions(txs: List[Payout]) -> TimeRangeSummary:
    if not txs:
        return TimeRangeSummary(
            total_payouts=0,
            num_payouts=0,
            largest_single_payout=0,
            average_payout_size=0,
        )
    total_payouts = sum(tx.value for tx in txs)
    num_payouts = len(txs)
    largest_single_payout = max(tx.value for tx in txs)
    average_payout_size = total_payouts / num_payouts
    return TimeRangeSummary(
        total_payouts=total_payouts,
        num_payouts=num_payouts,
        largest_single_payout=largest_single_payout,
        average_payout_size=average_payout_size,
    )


def process_transactions(transactions: List[Dict], wallet_address: str) -> FirmSummary:
    logger.info("Processing transactions for wallet: %s", wallet_address)
    outgoing_txs = [
        Payout(
            value=float(tx["value"]) / (10 ** int(tx["tokenDecimal"])),
            timestamp=datetime.fromtimestamp(int(tx["timeStamp"])),
        )
        for tx in transactions
        if tx["from"].lower() == wallet_address.lower()
    ]

    if not outgoing_txs:
        logger.warning("No outgoing transactions found for wallet: %s", wallet_address)
        return None

    # Sort transactions by timestamp, most recent first
    outgoing_txs.sort(key=lambda x: x.timestamp, reverse=True)

    # Sort transactions by value, largest first, for top 10 largest payouts
    top_10_largest = sorted(outgoing_txs, key=lambda x: x.value, reverse=True)[:10]

    now = datetime.now()
    last_24h = [tx for tx in outgoing_txs if (now - tx.timestamp) <= timedelta(days=1)]
    last_7d = [tx for tx in outgoing_txs if (now - tx.timestamp) <= timedelta(days=7)]
    last_30d = [tx for tx in outgoing_txs if (now - tx.timestamp) <= timedelta(days=30)]

    last_payout_timestamp = outgoing_txs[0].timestamp if outgoing_txs else now
    time_since_last_payout = calculate_time_since_last_payout(last_payout_timestamp)

    firm_summary = FirmSummary(
        name=next(
            firm["name"]
            for firm in FIRMS
            if firm["wallet"].lower() == wallet_address.lower()
        ),
        id=next(
            firm["id"]
            for firm in FIRMS
            if firm["wallet"].lower() == wallet_address.lower()
        ),
        last_24h=summarize_transactions(last_24h),
        last_7d=summarize_transactions(last_7d),
        last_30d=summarize_transactions(last_30d),
        all_time=summarize_transactions(outgoing_txs),
        last_10_payouts=outgoing_txs[:10],
        top_10_largest_payouts=top_10_largest,
        time_since_last_payout=time_since_last_payout,
    )
    logger.info(
        "Processed %d transactions for wallet: %s", len(outgoing_txs), wallet_address
    )
    return firm_summary


async def update_cache():
    while True:
        logger.info("Starting cache update")
        for firm in FIRMS:
            wallet = firm["wallet"].lower()
            try:
                transactions = await get_token_transactions(wallet)
                summary = process_transactions(transactions, wallet)
                if summary:
                    cache[wallet] = summary
                    logger.info("Updated cache for wallet: %s", wallet)
                else:
                    logger.warning("No summary generated for wallet: %s", wallet)
            except Exception as e:
                logger.error("Error updating cache for wallet %s: %s", wallet, str(e))

        logger.info(
            "Cache update completed. Next update in %d seconds", API_UPDATE_INTERVAL
        )
        await asyncio.sleep(API_UPDATE_INTERVAL)


@app.on_event("startup")
async def startup_event():
    logger.info("Starting up the application")
    asyncio.create_task(update_cache())


@app.get("/firms", response_model=List[FirmSummary])
async def get_firms_summary():
    logger.info("Received request for firms summary")
    summaries = []
    for firm in FIRMS:
        wallet = firm["wallet"].lower()
        summary = cache.get(wallet)
        if summary:
            summaries.append(summary)
        else:
            logger.warning(
                "No cached summary for wallet: %s. Fetching and processing...", wallet
            )
            transactions = await get_token_transactions(wallet)
            summary = process_transactions(transactions, wallet)
            if summary:
                cache[wallet] = summary
                summaries.append(summary)
            else:
                logger.error("Failed to generate summary for wallet: %s", wallet)

    logger.info("Returning summaries for %d firms", len(summaries))
    return summaries


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (Payout, TimeRangeSummary, FirmSummary)):
            return obj.dict()
        return super().default(obj)


# @app.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     logger.info("WebSocket connection attempt")
#     try:
#         await websocket.accept()
#         logger.info("WebSocket connection accepted")
#         try:
#             while True:
#                 summaries = [
#                     cache.get(firm["wallet"].lower())
#                     for firm in FIRMS
#                     if cache.get(firm["wallet"].lower())
#                 ]
#                 json_compatible_summaries = json.dumps(summaries, cls=CustomJSONEncoder)
#                 await websocket.send_text(json_compatible_summaries)

#                 logger.info("Data sent through WebSocket")
#                 await asyncio.sleep(WEBHOOK_UPDATE_INTERVAL)
#         except WebSocketDisconnect:
#             logger.info("WebSocket disconnected")
#         except ConnectionClosedOK:
#             logger.info("WebSocket connection closed normally")
#         except Exception as e:
#             logger.error("Error in WebSocket communication: %s", str(e))
#     except Exception as e:
#         logger.error("Failed to establish WebSocket connection: %s", str(e))


if __name__ == "__main__":
    import uvicorn

    logger.info("Starting the FastAPI application")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
