"""
Geyser monitoring for pump.fun tokens.
"""

import asyncio
from collections.abc import Awaitable, Callable

import grpc
from solders.pubkey import Pubkey

from geyser.generated import geyser_pb2, geyser_pb2_grpc
from monitoring.base_listener import BaseTokenListener
from monitoring.geyser_event_processor import GeyserEventProcessor
from trading.base import TokenInfo
from utils.logger import get_logger

logger = get_logger(__name__)

GRPC_CHANNEL_OPTIONS = [
    ('grpc.enable_retries', 0),
    ('grpc.keepalive_time_ms', 15000),
    ('grpc.keepalive_timeout_ms', 5000),
    ('grpc.keepalive_permit_without_calls', 1),
    ('grpc.http2.min_time_between_pings_ms', 15000),
    ('grpc.http2.max_pings_without_data', 0),
    # Consider ('grpc.max_receive_message_length', -1) if you expect very large messages, though default is usually fine.
]


class GeyserListener(BaseTokenListener):
    """Geyser listener for pump.fun token creation events."""

    def __init__(self, geyser_endpoint: str, geyser_api_token: str, geyser_auth_type: str, pump_program: Pubkey):
        """Initialize token listener.
        
        Args:
            geyser_endpoint: Geyser gRPC endpoint URL
            geyser_api_token: API token for authentication
            geyser_auth_type: authentication type ('x-token' or 'basic')
            pump_program: Pump.fun program address
        """
        self.geyser_endpoint = geyser_endpoint
        self.geyser_api_token = geyser_api_token
        # valid_auth_types = {"x-token", "basic"}
        # self.auth_type: str = (geyser_auth_type or "x-token").lower()
        # if self.auth_type not in valid_auth_types:
        #     raise ValueError(
        #         f"Unsupported auth_type={self.auth_type!r}. "
        #         f"Expected one of {valid_auth_types}"
        #     )
        # Connection state
        self.stub = None
        self.channel = None

        self.pump_program = pump_program
        self.event_processor = GeyserEventProcessor(pump_program)
        logger.info(f"Configuring Geyser for INSECURE channel to {self.geyser_endpoint}.")

    async def _create_geyser_connection(self):
        """Establish a secure connection to the Geyser endpoint."""
        channel = grpc.aio.insecure_channel(self.geyser_endpoint, options=GRPC_CHANNEL_OPTIONS)

        if not self.channel:  # Should not happen if above logic is correct
            raise ConnectionError(f"Failed to initialize gRPC channel to {self.geyser_endpoint}.")

        stub = geyser_pb2_grpc.GeyserStub(self.channel)
        logger.info(f"Geyser gRPC channel and stub initialized (Secure: {False}).")
        return stub, channel


    def _create_subscription_request(self):
        """Create a subscription request for Pump.fun transactions."""
        request = geyser_pb2.SubscribeRequest()
        request.transactions["pump_filter"].account_include.append(str(self.pump_program))
        request.transactions["pump_filter"].failed = False  # Only successful transactions
        request.commitment = geyser_pb2.CommitmentLevel.PROCESSED  # Crucial for speed
        logger.debug(
            f"Geyser subscription request configured for program {self.pump_program} with commitment {request.commitment}."
        )
        return request

    async def listen_for_tokens(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None = None,
        creator_address: str | None = None,
    ) -> None:
        """Listen for new token creations using Geyser subscription.
        
        Args:
            token_callback: Callback function for new tokens
            match_string: Optional string to match in token name/symbol
            creator_address: Optional creator address to filter by
        """
        stub, channel = await self._create_geyser_connection()
        request = self._create_subscription_request()

        while True:
            try:
                logger.info(f"Connected to Geyser endpoint: {self.geyser_endpoint}")
                logger.info(f"Monitoring for transactions involving program: {self.pump_program}")
                
                try:
                    async for update in stub.Subscribe(iter([request])):
                        if not update.HasField("transaction"):
                            if update.HasField("ping"):
                                logger.debug("Geyser ping received.")
                            continue
                        token_info = await self._process_update(update)
                        if not token_info:
                            continue
                            
                        logger.info(
                            f"New token detected: {token_info.name} ({token_info.symbol})"
                        )
    
                        if match_string and not (
                            match_string.lower() in token_info.name.lower()
                            or match_string.lower() in token_info.symbol.lower()
                        ):
                            logger.info(
                                f"Token does not match filter '{match_string}'. Skipping..."
                            )
                            continue
    
                        if (
                            creator_address
                            and str(token_info.user) != creator_address
                        ):
                            logger.info(
                                f"Token not created by {creator_address}. Skipping..."
                            )
                            continue
    
                        await token_callback(token_info)
                    logger.warning("Geyser Subscribe stream ended unexpectedly. Will attempt to reconnect.")

                except grpc.aio.AioRpcError as e:
                    logger.error(f"gRPC error during Subscribe: {e.details()} (Code: {e.code()})")
                    if self.channel:
                        await self.channel.close()  # Attempt to close gracefully
                    self.channel = None  # Force re-creation
                    self.stub = None
                    if e.code() == grpc.StatusCode.UNAUTHENTICATED:
                        logger.error("CRITICAL: gRPC UNAUTHENTICATED. Bot may stop if this persists.")
                        # Consider a different retry strategy or stopping the bot for auth errors.
                    await asyncio.sleep(5)
                    
                finally:
                    await channel.close()
                    
            except Exception as e:
                if self.channel:
                    await self.channel.close()
                self.channel = None
                self.stub = None
                logger.error(f"Geyser connection error: {e}")
                logger.info("Reconnecting in 10 seconds...")
                await asyncio.sleep(10)
    
    async def _process_update(self, update) -> TokenInfo | None:
        """Process a Geyser update and extract token creation info.
        
        Args:
            update: Geyser update from the subscription
            
        Returns:
            TokenInfo if a token creation is found, None otherwise
        """
        try:
            # # already checked by caller
            # if not update.HasField("transaction"):
            #     return None
                
            tx = update.transaction.transaction.transaction
            msg = getattr(tx, "message", None)
            if msg is None:
                return None

            for ix in msg.instructions:
                # Skip non-Pump.fun program instructions
                program_idx = ix.program_id_index
                if program_idx >= len(msg.account_keys):
                    continue
                    
                program_id = msg.account_keys[program_idx]
                if bytes(program_id) != bytes(self.pump_program):
                    continue

                # This is the most CPU-intensive part of _process_update typically.
                # Ensure GeyserEventProcessor is efficient.
                token_info = self.event_processor.process_transaction_data(
                    ix.data,  # Raw instruction data bytes
                    ix.accounts,  # List of u8 indices into account_keys_from_msg
                    msg.account_keys  # Full list of Pubkeys for the transaction
                )
                if token_info:
                    return token_info
                    
            return None
            
        except Exception as e:
            logger.error(f"Error processing Geyser update: {e}")
            return None
