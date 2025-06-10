"""
Main trading coordinator for pump.fun tokens.
Refactored PumpTrader to only process fresh tokens from WebSocket.
"""

import asyncio
import json
import os
from datetime import datetime
from time import monotonic
from typing import Optional

import uvloop
from solders.pubkey import Pubkey

from cleanup.modes import (
    handle_cleanup_after_failure,
    handle_cleanup_after_sell,
    handle_cleanup_post_session,
)
from core.client import SolanaClient
from core.curve import BondingCurveManager
from core.priority_fee.manager import PriorityFeeManager
from core.pubkeys import PumpAddresses
from core.wallet import Wallet
from monitoring.block_listener import BlockListener
from monitoring.geyser_listener import GeyserListener
from monitoring.logs_listener import LogsListener
from trading.base import TokenInfo, TradeResult
from trading.buyer import TokenBuyer
from trading.seller import TokenSeller
from utils.logger import get_logger

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logger = get_logger(__name__)


class PumpTrader:
    """Coordinates trading operations for pump.fun tokens with focus on freshness."""
    def __init__(
        self,
        rpc_endpoint: str,
        wss_endpoint: str,
        zero_slot_endpoint: str,
        private_key: str,
        buy_amount: float,
        token_amount: int,
        buy_slippage: float,
        sell_slippage: float,

        tip_lamports: int,
        tip_receiver: str,
        listener_type: str = "logs",

        geyser_endpoint: str | None = None,
        geyser_api_token: str | None = None,
        geyser_auth_type: str = "x-token",

        # Priority fee configuration
        enable_dynamic_priority_fee: bool = False,
        enable_fixed_priority_fee: bool = True,
        fixed_priority_fee: int = 200_000,
        extra_priority_fee: float = 0.0,
        hard_cap_prior_fee: int = 200_000,
        
        # Retry and timeout settings
        max_retries: int = 3,
        wait_time_after_creation: int = 0, # here and further - seconds
        wait_time_after_buy: float = 0.5,
        wait_time_before_new_token: int = 5,
        max_token_age: int | float = 0.5,
        token_wait_timeout: int = 30,
        
        # Cleanup settings
        cleanup_mode: str = "disabled",
        cleanup_force_close_with_burn: bool = False,
        cleanup_with_priority_fee: bool = False,
        
        # Trading filters
        match_string: str | None = None,
        bro_address: str | None = None,
        marry_mode: bool = False,
        yolo_mode: bool = False,
    ):
        """Initialize the pump trader.
        Args:
            rpc_endpoint: RPC endpoint URL
            wss_endpoint: WebSocket endpoint URL
            private_key: Wallet private key
            buy_amount: Amount of SOL to spend on buys
            buy_slippage: Slippage tolerance for buys
            sell_slippage: Slippage tolerance for sells

            listener_type: Type of listener to use ('logs', 'blocks', or 'geyser')
            geyser_endpoint: Geyser endpoint URL (required for geyser listener)
            geyser_api_token: Geyser API token (required for geyser listener)
            geyser_auth_type: Geyser authentication type ('x-token' or 'basic')

            enable_dynamic_priority_fee: Whether to enable dynamic priority fees
            enable_fixed_priority_fee: Whether to enable fixed priority fees
            fixed_priority_fee: Fixed priority fee amount
            extra_priority_fee: Extra percentage for priority fees
            hard_cap_prior_fee: Hard cap for priority fees

            max_retries: Maximum number of retry attempts
            wait_time_after_creation: Time to wait after token creation (seconds)
            wait_time_after_buy: Time to wait after buying a token (seconds)
            wait_time_before_new_token: Time to wait before processing a new token (seconds)
            max_token_age: Maximum age of token to process (seconds)
            token_wait_timeout: Timeout for waiting for a token in single-token mode (seconds)

            cleanup_mode: Cleanup mode ("disabled", "auto", or "manual")
            cleanup_force_close_with_burn: Whether to force close with burn during cleanup
            cleanup_with_priority_fee: Whether to use priority fees during cleanup
            
            match_string: Optional string to match in token name/symbol
            bro_address: Optional creator address to filter by
            marry_mode: If True, only buy tokens and skip selling
            yolo_mode: If True, trade continuously
        """
        self.solana_client = SolanaClient(rpc_endpoint=rpc_endpoint, zero_slot_endpoint=zero_slot_endpoint)
        self.wallet = Wallet(private_key)
        self.curve_manager = BondingCurveManager(self.solana_client)
        self.priority_fee_manager = PriorityFeeManager(
            client=self.solana_client,
            enable_dynamic_fee=enable_dynamic_priority_fee,
            enable_fixed_fee=enable_fixed_priority_fee,
            fixed_fee=fixed_priority_fee,
            extra_fee=extra_priority_fee,
            hard_cap=hard_cap_prior_fee,
        )
        self.buyer = TokenBuyer(
            client=self.solana_client,
            wallet=self.wallet,
            curve_manager=self.curve_manager,
            priority_fee_manager=self.priority_fee_manager,
            buy_sol_amount=buy_amount,
            token_amount=token_amount,
            slippage=buy_slippage,
            max_retries=max_retries,
            tip_lamports=tip_lamports,
            tip_receiver=tip_receiver,
        )
        self.seller = TokenSeller(
            client=self.solana_client,
            wallet=self.wallet,
            curve_manager=self.curve_manager,
            priority_fee_manager=self.priority_fee_manager,
            slippage=sell_slippage,
            max_retries=max_retries,
            token_balance=token_amount,
            tip_lamports=tip_lamports,
            tip_receiver=tip_receiver,
        )

        # Initialize the appropriate listener type
        listener_type = listener_type.lower()
        if listener_type == "geyser":
            if not geyser_endpoint or not geyser_api_token:
                logger.warning("Geyser listener selected but endpoint or API token might be missing. Ensure config is correct if auth is needed.")
                
            self.token_listener = GeyserListener(
                geyser_endpoint, 
                geyser_api_token,
                geyser_auth_type, 
                PumpAddresses.PROGRAM
            )
            logger.info("Using Geyser listener for token monitoring")
        elif listener_type == "logs":
            self.token_listener = LogsListener(wss_endpoint, PumpAddresses.PROGRAM)
            logger.info("Using logsSubscribe listener for token monitoring")
        else:
            self.token_listener = BlockListener(wss_endpoint, PumpAddresses.PROGRAM)
            logger.info("Using blockSubscribe listener for token monitoring")
            
        # Trading parameters
        self.buy_amount = buy_amount
        self.buy_slippage = buy_slippage
        self.sell_slippage = sell_slippage
        self.max_retries = max_retries

        # Timing parameters
        self.wait_time_after_creation = wait_time_after_creation
        self.wait_time_after_buy = wait_time_after_buy
        self.wait_time_before_new_token = wait_time_before_new_token
        self.max_token_age = max_token_age
        self.token_wait_timeout = token_wait_timeout
        
        # Cleanup parameters
        self.cleanup_mode = cleanup_mode
        self.cleanup_force_close_with_burn = cleanup_force_close_with_burn
        self.cleanup_with_priority_fee = cleanup_with_priority_fee

        # Trading filters/modes
        self.match_string = match_string
        self.bro_address = bro_address
        self.marry_mode = marry_mode
        self.yolo_mode = yolo_mode
        
        # State tracking
        self.traded_mints: set[Pubkey] = set()
        self.token_queue: asyncio.Queue = asyncio.Queue()
        self.processing: bool = False
        self.processed_tokens: set[str] = set()
        self.token_timestamps: dict[str, float] = {}
        
    async def start(self) -> None:
        """Start the trading bot and listen for new tokens."""
        logger.info("Starting pump.fun trader")
        logger.info(f"Match filter: {self.match_string if self.match_string else 'None'}")
        logger.info(f"Creator filter: {self.bro_address if self.bro_address else 'None'}")
        logger.info(f"Marry mode: {self.marry_mode}")
        logger.info(f"YOLO mode: {self.yolo_mode}")
        logger.info(f"Max token age: {self.max_token_age} seconds")

        # Starting RPC warmup procedured
        bot_name = "PumpFunTraderBot"

        logger.info(f"Background services for SolanaClient ({bot_name}) started.")

        try:
            health_resp = await self.solana_client.get_health()
            if health_resp:
                logger.info(f"RPC healthcheck successful (getHealth passed: {health_resp})")
            else:
                logger.warning(f"SolanaClient healthcheck failed for {bot_name}, proceeding.")

            if not await self.solana_client.get_cached_blockhash():
                logger.warning(f"SolanaClient warm-up failed for {bot_name}, proceeding.")
            logger.info(f"Starting background services for SolanaClient ({bot_name})...")
            # await self.solana_client.start_blockhash_updater()
            # await self.solana_client.start_oslot_keep_alive()

        except Exception as e:
            logger.warning(f"RPC warm-up failed: {e!s}")

        try:
            # Choose operating mode based on yolo_mode
            if not self.yolo_mode:
                # Single token mode: process one token and exit
                logger.info("Running in single token mode - will process one token and exit")
                token_info = await self._wait_for_token()
                if token_info:
                    await self._handle_token(token_info)
                    logger.info("Finished processing single token. Exiting...")
                else:
                    logger.info(f"No suitable token found within timeout period ({self.token_wait_timeout}s). Exiting...")
            else:
                # Continuous mode: process tokens until interrupted
                logger.info("Running in continuous mode - will process tokens until interrupted")
                processor_task = asyncio.create_task(
                    self._process_token_queue()
                )

                try:
                    await self.token_listener.listen_for_tokens(
                        self._queue_token,
                        self.match_string,
                        self.bro_address,
                    )
                except Exception as e:
                    logger.error(f"Token listening stopped due to error: {e!s}")
                finally:
                    processor_task.cancel()
                    try:
                        await processor_task
                    except asyncio.CancelledError:
                        pass
        
        except Exception as e:
            logger.error(f"Trading stopped due to error: {e!s}")
        
        finally:
            await self._cleanup_resources()
            logger.info("Pump trader has shut down")

    async def _wait_for_token(self) -> TokenInfo | None:
        """Wait for a single token to be detected.
        
        Returns:
            TokenInfo or None if timeout occurs
        """
        # Create a one-time event to signal when a token is found
        token_found = asyncio.Event()
        found_token: Optional[TokenInfo] = None
        
        async def token_callback(token: TokenInfo) -> None:
            nonlocal found_token
            token_key = str(token.mint)
            
            # Only process if not already processed and fresh
            if token_key not in self.processed_tokens:
                # Record when the token was discovered
                self.token_timestamps[token_key] = monotonic()
                found_token = token
                self.processed_tokens.add(token_key)
                token_found.set()
        
        listener_task = asyncio.create_task(
            self.token_listener.listen_for_tokens(
                token_callback,
                self.match_string,
                self.bro_address,
            )
        )
        
        # Wait for a token with a timeout
        try:
            logger.info(f"Waiting for a suitable token (timeout: {self.token_wait_timeout}s)...")
            await asyncio.wait_for(token_found.wait(), timeout=self.token_wait_timeout)
            logger.info(f"Found token: {found_token.symbol} ({found_token.mint})")
            return found_token
        except TimeoutError:
            logger.info(f"Timed out after waiting {self.token_wait_timeout}s for a token")
            return None
        finally:
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass

    async def _cleanup_resources(self) -> None:
        """Perform cleanup operations before shutting down."""
        if self.traded_mints:
            try:
                logger.info(f"Cleaning up {len(self.traded_mints)} traded token(s)...")
                await handle_cleanup_post_session(
                    self.solana_client, 
                    self.wallet, 
                    list(self.traded_mints), 
                    self.priority_fee_manager,
                    self.cleanup_mode,
                    self.cleanup_with_priority_fee,
                    self.cleanup_force_close_with_burn
                )
            except Exception as e:
                logger.error(f"Error during cleanup: {e!s}")
                
        old_keys = {k for k in self.token_timestamps if k not in self.processed_tokens}
        for key in old_keys:
            self.token_timestamps.pop(key, None)
            
        await self.solana_client.close()

    async def _queue_token(
        self, token_info: TokenInfo
    ) -> None:
        """Queue a token for processing if not already processed.
        
        Args:
            token_info: Token information to queue
        """
        token_key = str(token_info.mint)

        if token_key in self.processed_tokens:
            logger.debug(f"Token {token_info.symbol} already processed. Skipping...")
            return

        # Record timestamp when token was discovered
        self.token_timestamps[token_key] = monotonic()

        await self.token_queue.put(token_info)
        logger.info(f"Queued new token: {token_info.symbol} ({token_info.mint})")

    async def _process_token_queue(self) -> None:
        """Continuously process tokens from the queue, only if they're fresh."""
        while True:
            try:
                token_info = await self.token_queue.get()
                token_key = str(token_info.mint)

                # Check if token is still "fresh"
                current_time = monotonic()
                token_age = current_time - self.token_timestamps.get(
                    token_key, current_time
                )

                if token_age > self.max_token_age:
                    logger.info(
                        f"Skipping token {token_info.symbol} - too old ({token_age:.1f}s > {self.max_token_age}s)"
                    )
                    continue

                self.processed_tokens.add(token_key)

                logger.info(
                    f"Processing fresh token: {token_info.symbol} (age: {token_age:.1f}s)"
                )
                await self._handle_token(token_info)

            except asyncio.CancelledError:
                # Handle cancellation gracefully
                logger.info("Token queue processor was cancelled")
                break
            except Exception as e:
                logger.error(f"Error in token queue processor: {e!s}")
            finally:
                self.token_queue.task_done()

    async def _handle_token(
        self, token_info: TokenInfo
    ) -> None:
        """Handle a new token creation event.

        Args:
            token_info: Token information
        """
        try:
            # Buy token
            logger.info(
                f"Buying {self.buy_amount:.6f} SOL worth of {token_info.symbol}..."
            )
            buy_result: TradeResult = await self.buyer.execute(token_info)

            if buy_result.success:
                await self._handle_successful_buy(token_info, buy_result)
            else:
                await self._handle_failed_buy(token_info, buy_result)

            # Only wait for next token in yolo mode
            if self.yolo_mode:
                logger.info(
                    f"YOLO mode enabled. Waiting {self.wait_time_before_new_token} seconds before looking for next token..."
                )
                await asyncio.sleep(self.wait_time_before_new_token)

        except Exception as e:
            logger.error(f"Error handling token {token_info.symbol}: {e!s}")

    async def _handle_successful_buy(
        self, token_info: TokenInfo, buy_result: TradeResult
    ) -> None:
        """Handle successful token purchase.
        
        Args:
            token_info: Token information
            buy_result: The result of the buy operation
        """
        logger.info(f"Successfully bought {token_info.symbol}")
        self._log_trade(
            "buy",
            token_info,
            buy_result.price,  # type: ignore
            buy_result.amount,  # type: ignore
            buy_result.tx_signature,
        )
        self.traded_mints.add(token_info.mint)
        
        # Sell token if not in marry mode
        if not self.marry_mode:
            logger.info(
                f"Waiting for {self.wait_time_after_buy} seconds before selling..."
            )
            await asyncio.sleep(self.wait_time_after_buy)

            logger.info(f"Selling {token_info.symbol}...")
            sell_result: TradeResult = await self.seller.execute(token_info)

            if sell_result.success:
                logger.info(f"Successfully sold {token_info.symbol}")
                self._log_trade(
                    "sell",
                    token_info,
                    sell_result.price,  # type: ignore
                    sell_result.amount,  # type: ignore
                    sell_result.tx_signature,
                )
                # Close ATA if enabled
                await handle_cleanup_after_sell(
                    self.solana_client, 
                    self.wallet, 
                    token_info.mint, 
                    self.priority_fee_manager,
                    self.cleanup_mode,
                    self.cleanup_with_priority_fee,
                    self.cleanup_force_close_with_burn
                )
            else:
                logger.error(
                    f"Failed to sell {token_info.symbol}: {sell_result.error_message}"
                )
        else:
            logger.info("Marry mode enabled. Skipping sell operation.")

    async def _handle_failed_buy(
        self, token_info: TokenInfo, buy_result: TradeResult
    ) -> None:
        """Handle failed token purchase.
        
        Args:
            token_info: Token information
            buy_result: The result of the buy operation
        """
        logger.error(
            f"Failed to buy {token_info.symbol}: {buy_result.error_message}"
        )
        # Close ATA if enabled
        await handle_cleanup_after_failure(
            self.solana_client, 
            self.wallet, 
            token_info.mint, 
            self.priority_fee_manager,
            self.cleanup_mode,
            self.cleanup_with_priority_fee,
            self.cleanup_force_close_with_burn
        )

    async def _save_token_info(
        self, token_info: TokenInfo
    ) -> None:
        """Save token information to a file.

        Args:
            token_info: Token information
        """
        try:
            os.makedirs("trades", exist_ok=True)
            file_name = os.path.join("trades", f"{token_info.mint}.txt")

            with open(file_name, "w") as file:
                file.write(json.dumps(token_info.to_dict(), indent=2))

            logger.info(f"Token information saved to {file_name}")
        except Exception as e:
            logger.error(f"Failed to save token information: {e!s}")

    def _log_trade(
        self,
        action: str,
        token_info: TokenInfo,
        price: float,
        amount: float,
        tx_hash: str | None,
    ) -> None:
        """Log trade information.

        Args:
            action: Trade action (buy/sell)
            token_info: Token information
            price: Token price in SOL
            amount: Trade amount in SOL
            tx_hash: Transaction hash
        """
        try:
            os.makedirs("trades", exist_ok=True)

            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "action": action,
                "token_address": str(token_info.mint),
                "symbol": token_info.symbol,
                "price": price,
                "amount": amount,
                "tx_hash": str(tx_hash) if tx_hash else None,
            }

            with open("trades/trades.log", "a") as log_file:
                log_file.write(json.dumps(log_entry) + "\n")
        except Exception as e:
            logger.error(f"Failed to log trade information: {e!s}")