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
        "graph_id": "678e6bc22353fe1d2b323c02",
        "wallets": [
            "0xD3E70282420E6349bA3146bc299c4943f9782667",
            "0x0DB777559702d28509DAc1D7A8A473A0F53d96d6"
        ],
    },
    {
        "name": "Funding pips",
        "id": "66cb30fa8c725024fcd80ef2",
        "graph_id": "678e6bb63b3761d2b01fc9c5",
        "wallets": ["0x1e198Ad0608476EfA952De1cD8e574dB68df5f16"],
    },
    {
        "name": "Alpha capital",
        "id": "66cb31048376632dcc6a2a66",
        "graph_id": "678e6ba7e0ba1ad125137603",
        "wallets": ["0xD172B9C227361FCf6151e802e1F09C084964BDCD"],
    },
    {
        "name": "Funded Peaks",
        "id": "66cb310d796c0ab0a80632a3",
        "graph_id": "678e6b98b0e2307abe32a867",
        "wallets": ["0xd0D96d8Ad9c5f92b66A3b0d721c70D31da582C38"],
    },
    {
        "name": "MYFUNDEDFX",
        "id": "66cb6fb78376632dcc9fa7a3",
        "graph_id": "678e6b8dbb168b9cc8276dc8",
        "wallets": ["0x5534A2237f866F3089901A7D17f0E50dD7824c8D"],
    },
    {
        "name": "E8 markets",
        "id": "66cb6fc8d7c63944198a4e21",
        "graph_id": "678e6b80a22c7ac9286e0b85",
        "wallets": ["0xD19e945EEea57475B791C20112Ebb4B628f5B95c"],
    },
    {
        "name": "The 5ers",
        "id": "66cb6ff62a689130fc027367",
        "graph_id": "678e6b6ff923f65cd3724dc8",
        "wallets": ["0x349B0Ed1520eAE1472f57eaC77e390A1eCB0C677"],
    },
    {
        "name": "Pip farm",
        "id": "66cb7008ecc3e28071dc267c",
        "graph_id": "678e6b613b3761d2b01f7b1e",
        "wallets": ["0x442775D9FcbcD269bbdB7677ebd7f7D34dA5569F"],
    },
    {
        "name": "My flash funding",
        "id": "66cb70231238deea1b8e0dd3",
        "graph_id": "678e6b53b0e2307abe323f23",
        "wallets": ["0xeCa4DcC2D6B0ab4A496A3bDEC3cA07f95022cBdB"],
    },
    {
        "name": "Instant funding",
        "id": "66cb7033c6fa014536c41546",
        "graph_id": "678e6b476e503584b0461d19",
        "wallets": ["0xA3f21b162fA5a523e12590f2915cA418587Cf626"],
    },
    {
        "name": "Blue guardian",
        "id": "66cb70555230d82035fff546",
        "graph_id": "678e6b3217b57879096f709f",
        "wallets": ["0x68035843020B6c0CD94DD29c273FFF13c8e9A914"],
    },
    {
        "name": "ascend X",
        "id": "66cb6fa0ffd1354e499a8b2c",
        "graph_id": "678e6b21252cbe8b43570387",
        "wallets": ["0x858fcd36e5a8aBD64b955b1E2a70F3F75A464AFd"],
    },
    {
        "name": "FXIFY",
        "id": "66cb83bdedabdad8909e66f5",
        "graph_id": "678e6b09e2f8722ce43cb3ce",
        "wallets": ["0x36109F4D6804f391D830939c0C0e43EFc41a7486"],
    },
    {
        "name": "Funding Traders",
        "id": "66cba1ffecc3e2807105cd15",
        "graph_id": "678e6aefe71bcf32b9eaaf47",
        "wallets": ["0xe497F35cd0b928b686e9fb2e3cD131949D1841Cf"],
    },
    {
        "name": "Funded Next",
        "id": "66cc776ec15ebbd49e0b8f0c",
        "graph_id": "678e6adf496dfcedb4f88d97",
        "wallets": ["0x2b9a16e8448091159cc2b2a205b11f2368d53cb6"],
    },
    {
        "name": "Artic Funding",
        "id": "66cf2f4777b50737924ac87e",
        "graph_id": "678e6ad065d261e7a2de5fa7",
        "wallets": ["0xD4b7E42c1E0C0fDb4285d1dcfCe253C221a30b39"],
    },
    {
        "name": "TopTierTrader",
        "id": "66d26a7e7fa8b1bcae4c4bbc",
        "graph_id": "678e6ac2ec9e99fb1ee7e75f",
        "wallets": ["0x2265f428a8803bcc6Ccf4A405E627B0c51f33389"],
    },
    {
        "name": "My Crypto Funding",
        "id": "66f530e4e168c74898b83214",
        "graph_id": "678e6aabbb03f30e3393ba02",
        "wallets": ["0x390A8b588fAf26742f6C5433E29449B35bD35302"],
    },
    # {
    #     "name": "FastTrackTrading",
    #     "id": "66f5310482eacef08a74db7d",
    #     "wallet": "0x344B5330D8251e66723fd0DAA3eead06f0DfBfF7",
    # },
    {
        "name": "My Funded Futures",
        "id": "66f5311737c03a78ac3bba3b",
        "graph_id": "678e6a942948adbb1fd84a80",
        "wallets": ["0x9f28BaC1f793790dd3b0c2f8945F7cA28874f7A6"],
    },
    {
        "name": "Elite Trader Funding",
        "id": "66f531250b9009aedd25e1b1",
        "graph_id": "678e6a843bc9e80364bc803d",
        "wallets": ["0x8b028Fb68b277BBb5e6F231594771F010F123ddf"],
    },
    {
        "name": "Aqua Funded",
        "id": "66f5312de2de07e06d55f13f",
        "graph_id": "678e6a779e6a4ec581873306",
        "wallets": ["0x6F405a66cb4048fb05E72D74FCCcD5073697c469"],
    },
    {
        "name": "Monevis",
        "id": "66f700743428c39fd86601a0",
        "graph_id": "678e6a6a3374462018d2d13c",
        "wallets": ["0x7AF51b735Ef5AB57d6a9ebcA163FdE7A0A291c5b"],
    },
    {
        "name": "Goat Funded Futures",
        "id": "6751b8b3461eb55ecd1ebabd",
        "graph_id": "678e69ea3374462018d24e0d",
        "wallets": ["0x8732927164bfA58EE8AaB34043bDB41F13e8b1f1"],
    },
]

# Cache to store processed FirmSummary objects (24 hours TTL)
cache = TTLCache(maxsize=100, ttl=24 * 60 * 60)


class Payout(BaseModel):
    value: float
    timestamp: datetime
    wallet: str

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class TimeRangeSummary(BaseModel):
    total_payouts: float
    num_payouts: int
    largest_single_payout: float
    average_payout_size: float


class MonthlyPayouts(BaseModel):
    month: str  # Format: "YYYY-MM"
    total_amount: float

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class YearlyPayouts(BaseModel):
    total_amount: float
    monthly_breakdown: List[MonthlyPayouts]

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class FirmSummary(BaseModel):
    name: str
    id: str
    graph_id: str
    wallet: str
    last_24h: TimeRangeSummary
    last_7d: TimeRangeSummary
    last_30d: TimeRangeSummary
    previous_30d: TimeRangeSummary
    all_time: TimeRangeSummary
    last_10_payouts: List[Payout]
    top_10_largest_payouts: List[Payout]
    time_since_last_payout: Optional[str]
    percentage_change_from_previous_month: str
    monthly_payouts: List[MonthlyPayouts]

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


def process_transactions(
    transactions: List[Dict], wallets: List[str], firm: Dict
) -> FirmSummary:
    try:
        logger.info("Processing transactions for firm: %s", firm["name"])
        outgoing_txs = [
            Payout(
                value=float(tx["value"]) / (10 ** int(tx["tokenDecimal"])),
                timestamp=datetime.fromtimestamp(int(tx["timeStamp"])),
                wallet=tx["from"].lower(),
            )
            for tx in transactions
            if tx["from"].lower() in [wallet.lower() for wallet in wallets]
        ]

        if not outgoing_txs:
            return FirmSummary(
                name=firm["name"],
                id=firm["id"],
                graph_id=firm["graph_id"],
                wallet=", ".join(wallets),
                last_24h=TimeRangeSummary(
                    total_payouts=0,
                    num_payouts=0,
                    largest_single_payout=0,
                    average_payout_size=0,
                ),
                last_7d=TimeRangeSummary(
                    total_payouts=0,
                    num_payouts=0,
                    largest_single_payout=0,
                    average_payout_size=0,
                ),
                last_30d=TimeRangeSummary(
                    total_payouts=0,
                    num_payouts=0,
                    largest_single_payout=0,
                    average_payout_size=0,
                ),
                previous_30d=TimeRangeSummary(
                    total_payouts=0,
                    num_payouts=0,
                    largest_single_payout=0,
                    average_payout_size=0,
                ),
                all_time=TimeRangeSummary(
                    total_payouts=0,
                    num_payouts=0,
                    largest_single_payout=0,
                    average_payout_size=0,
                ),
                last_10_payouts=[],
                top_10_largest_payouts=[],
                time_since_last_payout=None,
                percentage_change_from_previous_month="0.00%",
                monthly_payouts=[],
            )

        # Sort transactions by timestamp, most recent first
        outgoing_txs.sort(key=lambda x: x.timestamp, reverse=True)

        # Get the earliest transaction date
        earliest_tx = min(outgoing_txs, key=lambda x: x.timestamp)
        earliest_date = earliest_tx.timestamp.replace(day=1)

        # Get current date
        now = datetime.now()
        current_date = now.replace(day=1)

        # Initialize monthly totals dictionary
        monthly_totals = {}

        # Initialize all months from earliest transaction to now
        temp_date = earliest_date
        while temp_date <= current_date:
            month_key = temp_date.strftime("%Y-%m")
            monthly_totals[month_key] = 0
            temp_date = (temp_date + timedelta(days=32)).replace(day=1)

        # Sum payouts by month
        for tx in outgoing_txs:
            month_key = tx.timestamp.strftime("%Y-%m")
            monthly_totals[month_key] = monthly_totals.get(month_key, 0) + tx.value

        monthly_payouts = [
            MonthlyPayouts(month=month, total_amount=amount)
            for month, amount in sorted(monthly_totals.items(), reverse=True)
        ]

        # Sort transactions by value, largest first, for top 10 largest payouts
        top_10_largest = sorted(outgoing_txs, key=lambda x: x.value, reverse=True)[:10]

        now = datetime.now()
        last_24h = [
            tx for tx in outgoing_txs if (now - tx.timestamp) <= timedelta(days=1)
        ]
        last_7d = [
            tx for tx in outgoing_txs if (now - tx.timestamp) <= timedelta(days=7)
        ]
        last_30d = [
            tx for tx in outgoing_txs if (now - tx.timestamp) <= timedelta(days=30)
        ]
        previous_30d = [
            tx
            for tx in outgoing_txs
            if timedelta(days=30) < (now - tx.timestamp) <= timedelta(days=60)
        ]

        last_payout_timestamp = outgoing_txs[0].timestamp if outgoing_txs else now
        time_since_last_payout = calculate_time_since_last_payout(last_payout_timestamp)

        last_30d_summary = summarize_transactions(last_30d)
        previous_30d_summary = summarize_transactions(previous_30d)

        if previous_30d_summary.total_payouts > 0:
            percentage_change = (
                (last_30d_summary.total_payouts - previous_30d_summary.total_payouts)
                / previous_30d_summary.total_payouts
            ) * 100
            percentage_change_str = (
                f"+{percentage_change:.2f}%"
                if percentage_change > 0
                else f"{percentage_change:.2f}%"
            )
        else:
            percentage_change = 100 if last_30d_summary.total_payouts > 0 else 0
            percentage_change_str = (
                f"+{percentage_change:.2f}%"
                if percentage_change > 0
                else f"{percentage_change:.2f}%"
            )

        firm_summary = FirmSummary(
            name=firm["name"],
            id=firm["id"],
            graph_id=firm["graph_id"],
            wallet=", ".join(wallets),
            last_24h=summarize_transactions(last_24h),
            last_7d=summarize_transactions(last_7d),
            last_30d=last_30d_summary,
            previous_30d=previous_30d_summary,
            all_time=summarize_transactions(outgoing_txs),
            last_10_payouts=outgoing_txs[:10],
            top_10_largest_payouts=top_10_largest,
            time_since_last_payout=time_since_last_payout,
            percentage_change_from_previous_month=percentage_change_str,
            monthly_payouts=monthly_payouts,
        )
        return firm_summary

    except Exception as e:
        logger.error(f"Error processing transactions for firm {firm['name']}: {str(e)}")
        logger.error(
            f"Transactions data: {transactions[:2]}..."
        )  # Log first few transactions for debugging
        raise  # Re-raise the exception for higher-level handling


async def update_cache():
    while True:
        logger.info("Starting cache update")
        for firm in FIRMS:
            all_transactions = []
            wallets = (
                firm["wallets"]
                if isinstance(firm["wallets"], list)
                else [firm["wallets"]]
            )

            for wallet in wallets:
                wallet = wallet.lower()
                try:
                    transactions = await get_token_transactions(wallet)
                    all_transactions.extend(transactions)
                except Exception as e:
                    logger.error(
                        "Error fetching transactions for wallet %s: %s", wallet, str(e)
                    )

            if all_transactions:
                try:
                    summary = process_transactions(all_transactions, wallets, firm)
                    if summary:
                        cache[firm["id"]] = summary
                        logger.info("Updated cache for firm: %s", firm["name"])
                    else:
                        logger.warning(
                            "No summary generated for firm: %s", firm["name"]
                        )
                except Exception as e:
                    logger.error(
                        "Error processing transactions for firm %s: %s",
                        firm["name"],
                        str(e),
                    )

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
        try:
            summary = cache.get(firm["id"])
            if summary:
                summaries.append(summary)
                continue

            logger.warning(
                "No cached summary for firm: %s. Fetching and processing...",
                firm["name"],
            )
            all_transactions = []
            wallets = (
                firm["wallets"]
                if isinstance(firm["wallets"], list)
                else [firm["wallets"]]
            )

            for wallet in wallets:
                wallet = wallet.lower()
                try:
                    transactions = await get_token_transactions(wallet)
                    all_transactions.extend(transactions)
                except Exception as e:
                    logger.error(
                        "Error fetching transactions for wallet %s: %s", wallet, str(e)
                    )

            if all_transactions:
                try:
                    summary = process_transactions(all_transactions, wallets, firm)
                    if summary:
                        cache[firm["id"]] = summary
                        summaries.append(summary)
                    else:
                        logger.warning(
                            "No summary generated for firm: %s", firm["name"]
                        )
                except Exception as e:
                    logger.error(
                        "Error processing transactions for firm %s: %s",
                        firm["name"],
                        str(e),
                    )

        except Exception as e:
            logger.error(f"Error processing firm {firm['name']}: {str(e)}")
            continue  # Skip this firm and continue with the next one

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
#                     cache.get(firm["wallets"][0])
#                     for firm in FIRMS
#                     if cache.get(firm["wallets"][0])
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


@app.get("/monthly-payouts/{firm_id}", response_model=List[MonthlyPayouts])
async def get_monthly_payouts(firm_id: str):
    """Get monthly payout totals for the last 12 months for a specific firm."""
    # Get data from cache
    if "firms" not in cache:
        await update_cache()

    firms_data = cache.get("firms", [])

    # Find the firm by ID
    firm_data = next((firm for firm in firms_data if firm.id == firm_id), None)
    if not firm_data:
        raise HTTPException(status_code=404, detail="Firm not found")

    # Get all payouts for the firm
    all_payouts = firm_data.last_10_payouts + firm_data.top_10_largest_payouts
    # Remove duplicates by converting to dict using timestamp as key
    unique_payouts = {
        payout.timestamp.strftime("%Y-%m-%d %H:%M:%S"): payout for payout in all_payouts
    }
    payouts = list(unique_payouts.values())

    # Get current date and date 12 months ago
    now = datetime.now()
    twelve_months_ago = now - timedelta(days=365)

    # Initialize monthly totals dictionary
    monthly_totals = {}

    # Initialize all months with zero
    current_date = twelve_months_ago
    while current_date <= now:
        month_key = current_date.strftime("%Y-%m")
        monthly_totals[month_key] = 0
        current_date += timedelta(
            days=32
        )  # Move to next month (32 days ensures we move to next month)
        current_date = current_date.replace(day=1)  # Reset to first day of month

    # Sum payouts by month
    for payout in payouts:
        if payout.timestamp >= twelve_months_ago:
            month_key = payout.timestamp.strftime("%Y-%m")
            monthly_totals[month_key] += payout.value

    # Convert to list of MonthlyPayouts objects
    result = [
        MonthlyPayouts(month=month, total_amount=amount)
        for month, amount in sorted(monthly_totals.items())
    ]

    return result


if __name__ == "__main__":
    import uvicorn

    logger.info("Starting the FastAPI application")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
