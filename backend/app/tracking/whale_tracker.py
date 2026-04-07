"""Whale / Smart Money tracker: scans Polygon on-chain data for large CTF transfers."""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Outcome
from app.models.whale import WalletActivity, WalletProfile

logger = logging.getLogger(__name__)

# Polymarket CTF contract on Polygon
CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# Polygonscan Transfer event topic (ERC-1155 TransferSingle)
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"


async def _fetch_large_transfers_polygonscan(hours: int = 1) -> list[dict]:
    """Fetch large CTF Transfer events from Polygonscan API.

    Returns raw transfer dicts with: from, to, token_id, value, tx_hash, block_number, timestamp.
    """
    if not settings.polygon_rpc_url:
        logger.debug("whale_tracker: no polygon_rpc_url configured, skipping Polygonscan fetch")
        return []

    # Use Polygonscan API to get recent transfer events
    now = datetime.now(timezone.utc)
    start_ts = int((now - timedelta(hours=hours)).timestamp())

    params = {
        "module": "logs",
        "action": "getLogs",
        "address": CTF_CONTRACT,
        "topic0": TRANSFER_SINGLE_TOPIC,
        "fromBlock": "latest",
        "toBlock": "latest",
        "apikey": settings.polygon_rpc_url,  # polygon_rpc_url doubles as API key for Polygonscan
    }

    transfers = []
    try:
        async with httpx.AsyncClient(timeout=settings.connector_timeout_seconds) as client:
            resp = await client.get("https://api.polygonscan.com/api", params=params)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "1" or not data.get("result"):
                logger.debug("whale_tracker: no transfer events from Polygonscan")
                return []

            for log in data["result"]:
                try:
                    # Decode TransferSingle event data
                    # topics: [event_sig, operator, from, to]
                    # data: [token_id, value]
                    topics = log.get("topics", [])
                    if len(topics) < 4:
                        continue

                    from_addr = "0x" + topics[2][-40:]
                    to_addr = "0x" + topics[3][-40:]
                    hex_data = log.get("data", "0x")

                    # data contains token_id (uint256) and value (uint256) — each 32 bytes
                    if len(hex_data) < 130:  # 0x + 64 + 64
                        continue

                    token_id = str(int(hex_data[2:66], 16))
                    value = int(hex_data[66:130], 16)

                    block_number = int(log.get("blockNumber", "0x0"), 16)
                    timestamp_hex = log.get("timeStamp", "0x0")
                    timestamp = int(timestamp_hex, 16) if timestamp_hex.startswith("0x") else int(timestamp_hex)

                    # Filter: only transfers after our window
                    if timestamp < start_ts:
                        continue

                    transfers.append({
                        "from": from_addr.lower(),
                        "to": to_addr.lower(),
                        "token_id": token_id,
                        "value": value,
                        "tx_hash": log.get("transactionHash", ""),
                        "block_number": block_number,
                        "timestamp": datetime.fromtimestamp(timestamp, tz=timezone.utc),
                    })
                except (ValueError, IndexError):
                    continue

    except httpx.HTTPError:
        logger.warning("whale_tracker: Polygonscan API request failed", exc_info=True)

    return transfers


async def _map_token_to_outcome(session: AsyncSession, token_id: str) -> Outcome | None:
    """Map a CTF token ID to a known Outcome in the database."""
    result = await session.execute(
        select(Outcome).where(Outcome.token_id == token_id).limit(1)
    )
    return result.scalar_one_or_none()


async def _get_or_create_wallet(session: AsyncSession, address: str) -> WalletProfile:
    """Get existing wallet profile or create a new one."""
    result = await session.execute(
        select(WalletProfile).where(WalletProfile.address == address)
    )
    wallet = result.scalar_one_or_none()
    if wallet is None:
        wallet = WalletProfile(address=address)
        session.add(wallet)
        await session.flush()
    return wallet


async def _update_wallet_stats(
    session: AsyncSession, wallet: WalletProfile, notional_usd: Decimal, timestamp: datetime
) -> None:
    """Update wallet cumulative stats after a new activity."""
    wallet.total_volume += notional_usd
    wallet.trade_count += 1
    if wallet.last_active is None or timestamp > wallet.last_active:
        wallet.last_active = timestamp


async def _auto_track_wallet(wallet: WalletProfile) -> bool:
    """Check if wallet meets auto-tracking thresholds and update tracked flag."""
    min_volume = Decimal(str(settings.whale_min_volume_usd))
    min_win_rate = Decimal(str(settings.whale_min_win_rate))

    if wallet.total_volume >= min_volume:
        if wallet.win_rate is None or wallet.win_rate >= min_win_rate:
            if not wallet.tracked:
                wallet.tracked = True
                logger.info("whale_tracker: auto-tracked wallet %s (volume=$%.2f)", wallet.address, wallet.total_volume)
                return True
    return False


async def scan_recent_activity(session: AsyncSession, hours: int = 1) -> list[WalletActivity]:
    """Fetch large transfers from on-chain data, map to markets, persist activities.

    Returns list of new WalletActivity records for signal generation.
    """
    transfers = await _fetch_large_transfers_polygonscan(hours=hours)
    if not transfers:
        return []

    new_activities: list[WalletActivity] = []

    for transfer in transfers:
        tx_hash = transfer["tx_hash"]
        if not tx_hash:
            continue

        # Check idempotency — skip if tx_hash already recorded
        existing = await session.execute(
            select(WalletActivity.id).where(WalletActivity.tx_hash == tx_hash).limit(1)
        )
        if existing.scalar_one_or_none() is not None:
            continue

        # Map token to outcome
        outcome = await _map_token_to_outcome(session, transfer["token_id"])

        # Determine action: mint address = buy, burn = sell
        zero_addr = "0x" + "0" * 40
        if transfer["from"] == zero_addr:
            action = "buy"
            wallet_address = transfer["to"]
        elif transfer["to"] == zero_addr:
            action = "sell"
            wallet_address = transfer["from"]
        else:
            # Regular transfer — treat receiver as buyer
            action = "buy"
            wallet_address = transfer["to"]

        # Estimate notional value (value * price if we have outcome price, else value / 1e6 as USDC)
        raw_value = Decimal(str(transfer["value"]))
        # CTF tokens are typically in 1e6 precision
        quantity = raw_value / Decimal("1000000")
        notional_usd = quantity  # 1 share ~= 1 USDC at par; adjust with price if available

        # Get or create wallet profile
        wallet = await _get_or_create_wallet(session, wallet_address)

        # Create activity
        activity = WalletActivity(
            wallet_id=wallet.id,
            outcome_id=outcome.id if outcome else None,
            action=action,
            quantity=quantity,
            price=None,  # price from on-chain is not directly available
            notional_usd=notional_usd,
            tx_hash=tx_hash,
            block_number=transfer["block_number"],
            timestamp=transfer["timestamp"],
        )
        session.add(activity)

        # Update wallet stats
        await _update_wallet_stats(session, wallet, notional_usd, transfer["timestamp"])

        # Auto-track check
        await _auto_track_wallet(wallet)

        new_activities.append(activity)

    if new_activities:
        await session.commit()
        logger.info("whale_tracker: persisted %d new activities", len(new_activities))

    return new_activities
