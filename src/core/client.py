"""
Solana client abstraction for blockchain operations.
"""

import asyncio
import json
from typing import Any, Optional
from aiocache import cached, Cache

import aiohttp
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Processed, Commitment
from solana.rpc.types import TxOpts

from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.hash import Hash
from solders.instruction import Instruction
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.solders import RECENT_BLOCKHASHES
from solders.signature import Signature
from solders.transaction import VersionedTransaction
from solders.system_program import TransferParams, transfer as system_transfer

from utils.logger import get_logger

logger = get_logger(__name__)


class SolanaClient:
    """Abstraction for Solana RPC client operations."""

    def __init__(self, rpc_endpoint: str, zero_slot_endpoint: str) -> None:
        """Initialize Solana client with RPC endpoint.

        Args:
            rpc_endpoint: URL of the Solana RPC endpoint
        """
        self.rpc_endpoint = rpc_endpoint
        self._client = AsyncClient(self.rpc_endpoint)

        self._0slot_endpoint = zero_slot_endpoint
        self._0slot_send_client = AsyncClient(zero_slot_endpoint)

        self._cached_blockhash: Hash | None = None
        self._blockhash_lock = asyncio.Lock()
        self._blockhash_updater_task = asyncio.create_task(self.start_blockhash_updater())

    async def start_blockhash_updater(self, interval: float = 5.0):
        """Start background task to update recent blockhash."""
        while True:
            try:
                blockhash = await self.get_latest_blockhash()
                async with self._blockhash_lock:
                    self._cached_blockhash = blockhash
            except Exception as e:
                logger.warning(f"Blockhash fetch failed: {e!s}")
            finally:
                await asyncio.sleep(interval)

    @cached(ttl=90, cache=Cache.MEMORY)
    async def get_cached_blockhash(self) -> Hash:
        """Return the most recently cached blockhash."""
        async with self._blockhash_lock:
            logger.info(f"Blockhash invalidated after 90 sec. Updating blockhash...")
            return await self.get_latest_blockhash()

    async def get_client(self) -> AsyncClient:
        """Get or create the AsyncClient instance.

        Returns:
            AsyncClient instance
        """
        if self._client is None:
            self._client = AsyncClient(self.rpc_endpoint)
        return self._client

    async def close(self):
        """Close the client connection and stop the blockhash updater."""
        if self._blockhash_updater_task:
            self._blockhash_updater_task.cancel()
            try:
                await self._blockhash_updater_task
            except asyncio.CancelledError:
                pass

        if self._client:
            await self._client.close()
            self._client = None

    async def get_health(self) -> str | None:
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getHealth",
        }
        result = await self.post_rpc(body)
        if result and "result" in result:
            return result["result"]
        return None

    async def get_account_info(self, pubkey: Pubkey) -> dict[str, Any]:
        """Get account info from the blockchain.

        Args:
            pubkey: Public key of the account

        Returns:
            Account info response

        Raises:
            ValueError: If account doesn't exist or has no data
        """
        client = await self.get_client()
        response = await client.get_account_info(pubkey, encoding="base64") # base64 encoding for account data by default
        if not response.value:
            raise ValueError(f"Account {pubkey} not found")
        return response.value

    async def get_token_account_balance(self, token_account: Pubkey) -> int:
        """Get token balance for an account.

        Args:
            token_account: Token account address

        Returns:
            Token balance as integer
        """
        client = await self.get_client()
        response = await client.get_token_account_balance(token_account)
        if response.value:
            return int(response.value.amount)
        return 0

    async def get_latest_blockhash(self) -> Hash:
        """Get the latest blockhash.

        Returns:
            Recent blockhash as string
        """
        client = await self.get_client()
        response = await client.get_latest_blockhash(commitment="processed")
        return response.value.blockhash

    async def build_and_send_transaction(
        self,
        instructions: list[Instruction],
        signer_keypair: Keypair,
        skip_preflight: bool = True,
        max_retries: int = 3,
        priority_fee: int | None = None,
        compute_unit_limit: Optional[int] = None,
        tip_receiver: Pubkey = None,
        tip_lamports: int = None,
    ) -> str:
        """
        Send a transaction with optional priority fee.

        Args:
            instructions: List of instructions to include in the transaction.
            skip_preflight: Whether to skip preflight checks.
            max_retries: Maximum number of retry attempts.
            priority_fee: Optional priority fee in microlamports.

        Returns:
            Transaction signature.
        """
        client = self._0slot_send_client

        logger.info(
            f"Priority fee in microlamports: {priority_fee if priority_fee else 0}"
        )

        final_ordered_instructions = []
        # 1. Compute Budget Instructions (must be first)
        if priority_fee and priority_fee > 0:
            final_ordered_instructions.append(set_compute_unit_price(micro_lamports=priority_fee))
        if compute_unit_limit and compute_unit_limit > 0:
            final_ordered_instructions.append(set_compute_unit_limit(units=compute_unit_limit))

        # 2. 0slot Tip Instruction (if applicable, comes after CU, before main instructions)

        if tip_lamports < 1_000_000:  # As per 0slot docs >= 0.001 SOL
            logger.warning(
                f"0slot tip lamports is {tip_lamports}, which is less than the recommended 1,000,000 (0.001 SOL).")

        tip_instruction = system_transfer(
            TransferParams(
                from_pubkey=signer_keypair.pubkey(),
                to_pubkey=tip_receiver,
                lamports=tip_lamports
            )
        )
        final_ordered_instructions.append(tip_instruction)
        # 3. Main transaction instructions
        final_ordered_instructions.extend(instructions)

        recent_blockhash = await self.get_cached_blockhash()
        message = Message.new_with_blockhash(
            instructions=final_ordered_instructions,
            payer=signer_keypair.pubkey(),
            blockhash=recent_blockhash
        )
        transaction = VersionedTransaction(message=message, keypairs=[signer_keypair])

        for attempt in range(max_retries):
            try:
                tx_opts = TxOpts(
                    skip_preflight=skip_preflight, preflight_commitment=Processed, max_retries=max_retries
                )
                response = await client.send_transaction(transaction, tx_opts)
                tx_signature_str = str(response.value)
                logger.info(f"Transaction sent via 0slot. Signature: {tx_signature_str}")
                return tx_signature_str

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(
                        f"Failed to send transaction after {max_retries} attempts"
                    )
                    raise

                wait_time = 1.0 + (0.5 * attempt)
                logger.warning(
                    f"Transaction attempt {attempt + 1} failed: {e!s}, retrying in {wait_time}s"
                )
                await asyncio.sleep(wait_time)

    async def confirm_transaction(
        self, signature: str, commitment: Commitment = Processed
    ) -> bool:
        """Wait for transaction confirmation.

        Args:
            signature: Transaction signature
            commitment: Confirmation commitment level

        Returns:
            Whether transaction was confirmed
        """
        client = await self.get_client()
        try:
            await client.confirm_transaction(Signature.from_string(signature), commitment=commitment, sleep_seconds=1)
            return True
        except Exception as e:
            logger.error(f"Failed to confirm transaction {signature}: {e!s}")
            return False

    async def post_rpc(self, body: dict[str, Any]) -> dict[str, Any] | None:
        """
        Send a raw RPC request to the Solana node.

        Args:
            body: JSON-RPC request body.

        Returns:
            Optional[Dict[str, Any]]: Parsed JSON response, or None if the request fails.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.rpc_endpoint,
                    json=body,
                    timeout=aiohttp.ClientTimeout(10),  # 10-second timeout
                ) as response:
                    response.raise_for_status()
                    return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"RPC request failed: {e!s}", exc_info=True)
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode RPC response: {e!s}", exc_info=True)
            return None

    # ... (0slot keep-alive methods remain the same as your version) ...
    # async def _perform_oslot_keep_alive(self):
    #     if not self._0slot_send_client or not self._0slot_endpoint:
    #         logger.error("0slot client or base URL not configured. Cannot start keep-alive.")
    #         self._keep_alive_running = False
    #         return
    #     logger.info(f"0slot Keep-Alive task started for {self._0slot_endpoint}. Interval: {OSLOT_KEEPALIVE_INTERVAL_SECONDS}s.")
    #     initial_ping_done = False
    #     while self._keep_alive_running:
    #         try:
    #             connected = await self._0slot_send_client.is_connected()
    #             if connected:
    #                 if not initial_ping_done:
    #                     logger.info(f"0slot Keep-Alive: Initial ping to {self._0slot_endpoint} successful.")
    #                     initial_ping_done = True
    #             else:
    #                 logger.warning(f"0slot Keep-Alive: Ping to {self._0slot_endpoint} failed (is_connected returned False).")
    #             await asyncio.sleep(OSLOT_KEEPALIVE_INTERVAL_SECONDS)
    #         except asyncio.CancelledError:
    #             logger.info("0slot Keep-Alive task has been cancelled.")
    #             break
    #         except SolanaRpcException as e:
    #             logger.error(f"0slot Keep-Alive: RPC Exception during ping to {self._0slot_endpoint}: {e!r}")
    #             await asyncio.sleep(OSLOT_KEEPALIVE_INTERVAL_SECONDS / 2)
    #         except Exception as e:
    #             logger.error(f"0slot Keep-Alive: Unexpected error during ping to {self._0slot_endpoint}: {e!r}", exc_info=self.debug_mode)
    #             await asyncio.sleep(OSLOT_KEEPALIVE_INTERVAL_SECONDS / 2)
    #     logger.info(f"0slot Keep-Alive task for {self._0slot_endpoint} has stopped.")
    #
    # async def start_oslot_keep_alive(self):
    #     if not self._oslot_keep_alive_task or self._oslot_keep_alive_task.done():
    #         if not self._keep_alive_running: # Ensure flag is reset if task was done
    #             self._keep_alive_running = True
    #         self._oslot_keep_alive_task = asyncio.create_task(self._perform_oslot_keep_alive())
    #         await asyncio.sleep(0.1)
    #     else:
    #         logger.debug("0slot Keep-Alive task is already running or was previously started.")


