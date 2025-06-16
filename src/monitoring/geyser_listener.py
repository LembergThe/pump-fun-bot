"""
Geyser monitoring for pump.fun tokens.
"""

import asyncio
from typing import Optional, Dict, List
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import time

import grpc
from solders.pubkey import Pubkey
from solders.signature import Signature

from monitoring.base_listener import BaseTokenListener
from monitoring.geyser_event_processor import GeyserEventProcessor
from trading.base import TokenInfo
from utils.logger import get_logger

from src.geyser.generated import geyser_pb2, geyser_pb2_grpc
from src.geyser.generated.solana_storage_pb2 import (
    InnerInstructions as GrpcInnerInstructions,
)
from src.geyser.generated.solana_storage_pb2 import (
    Message as GrpcMessage,
)
from src.geyser.generated.solana_storage_pb2 import (
    MessageHeader as GrpcMessageHeader,
)
from src.geyser.generated.solana_storage_pb2 import (
    Transaction as GrpcTransactionPayload,
)
from src.geyser.generated.solana_storage_pb2 import (
    TransactionStatusMeta as GrpcTransactionStatusMeta,
)
from src.geyser.generated.geyser_pb2 import SubscribeUpdate, SubscribeUpdateTransaction


logger = get_logger(__name__)

GRPC_CHANNEL_OPTIONS = [
    ('grpc.enable_retries',0),
    ('grpc.keepalive_time_ms',15000),
    ('grpc.keepalive_timeout_ms',5000),
    ('grpc.keepalive_permit_without_calls',1),
    ('grpc.http2.min_time_between_pings_ms',15000),
    ('grpc.http2.max_pings_without_data',0),
    ('grpc.max_receive_message_length',1024*1024*100),
    ('grpc.max_send_message_length',1024*1024*100)
]

class PumpFunData:
    CREATE_IX_DISCRIMINATOR = bytes([24,30,200,40,5,28,7,119])
    SELL_IX_DISCRIMINATOR = bytes([51,230,133,164,1,127,131,173])
    BUY_IX_DISCRIMINATOR = bytes([102,6,61,18,1,218,235,234])


@dataclass
class NewTokenTrackedInfo:
    """Represents the essential information for a newly created token."""
    mint: Pubkey
    developer_signer: Optional[Pubkey]
    developer_token_account_after_creation: Optional[Pubkey]
    name: str
    symbol: str
    uri: Optional[str]
    bonding_curve: Optional[Pubkey]
    associated_bonding_curve: Optional[Pubkey]
    creator_arg: Optional[Pubkey]
    decimals: int
    transaction_signature: Signature
    block_time: Optional[int] = None
    detection_timestamp: float = field(default_factory=time.perf_counter)
    dev_tokens_from_creation_event_raw: Optional[int] = None
    dev_initial_buy_sol: Optional[float] = None
    dev_remaining_tokens_from_creation_raw: Optional[int] = field(init=False, default=None)
    creator_vault: Optional[Pubkey] = None
    num_signers: int = 1

    def __post_init__(self):
        """Initializes fields that depend on other fields."""
        self.dev_remaining_tokens_from_creation_raw = self.dev_tokens_from_creation_event_raw

    def __repr__(self) -> str:
        """Provides a concise representation of the tracked token."""
        dev_buy_repr = (
            f"{self.dev_initial_buy_sol:.4f} SOL"
            if self.dev_initial_buy_sol is not None
            else "None"
        )
        return (
            f"NewTokenTrackedInfo(mint='{self.mint}', sym='{self.symbol}', "
            f"dev_init_buy={dev_buy_repr}, create_tx='{self.transaction_signature}')"
        )


class GeyserListener(BaseTokenListener):
    """Geyser listener for pump.fun token creation events."""

    def __init__(
        self,
        geyser_endpoint: str,
        geyser_api_token: str,
        pump_program: Pubkey,
        max_sol_cost_realization_factor: float = 1.0,
        min_dev_buy_in_sol: Optional[float] = None,
        max_dev_buy_in_sol: Optional[float] = None,
        token_onchain_time_max: Optional[int] = None,
    ):
        """Initialize token listener.
        
        Args:
            geyser_endpoint: Geyser gRPC endpoint URL
            geyser_api_token: API token for authentication
            geyser_auth_type: authentication type ('x-token' or 'basic')
            pump_program: Pump.fun program address
        """
        self.geyser_endpoint = geyser_endpoint
        self.geyser_api_token = geyser_api_token

        # Connection state
        self.stub: Optional[geyser_pb2_grpc.GeyserStub] = None
        self.channel: Optional[grpc.aio.Channel] = None

        self.max_sol_cost_realization_factor = max_sol_cost_realization_factor
        self.min_dev_buy_in_sol = min_dev_buy_in_sol
        self.max_dev_buy_in_sol = max_dev_buy_in_sol
        self.token_onchain_time_max = token_onchain_time_max

        self.pump_program = pump_program
        self.event_processor = GeyserEventProcessor(pump_program)
        logger.info(f"Configuring Geyser for INSECURE channel to {self.geyser_endpoint}.")

        self.created_tokens_info: Dict[Pubkey, NewTokenTrackedInfo] = {}


    async def _create_geyser_connection(self):
        """Establish a secure connection to the Geyser endpoint."""
        channel = grpc.aio.insecure_channel(self.geyser_endpoint, options=GRPC_CHANNEL_OPTIONS)

        if not channel:  # Should not happen if above logic is correct
            raise ConnectionError(f"Failed to initialize gRPC channel to {self.geyser_endpoint}.")

        stub = geyser_pb2_grpc.GeyserStub(channel)
        logger.info(f"Geyser gRPC channel and stub initialized (Secure: {False}).")
        return stub, channel


    def _create_subscription_request(self) -> geyser_pb2.SubscribeRequest:
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
        self.stub, self.channel = await self._create_geyser_connection()
        request = self._create_subscription_request()

        while True:
            try:
                logger.info(f"Connected to Geyser endpoint: {self.geyser_endpoint}")
                logger.info(f"Monitoring for transactions involving program: {self.pump_program}")
                
                try:
                    async for update in self.stub.Subscribe(iter([request])):
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
                    await self.channel.close()
                    
            except Exception as e:
                if self.channel:
                    await self.channel.close()
                self.channel = None
                self.stub = None
                logger.error(f"Geyser connection error: {e}")
                logger.info("Reconnecting in 10 seconds...")
                await asyncio.sleep(10)
    
    async def _process_update(self, update: SubscribeUpdate) -> TokenInfo | None:
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
            logger.info(f"Received update: {update}")
            tx: GrpcTransactionPayload = update.transaction.transaction.transaction
            tx_meta: GrpcTransactionStatusMeta = update.transaction.transaction.meta
            tx_block_time: Optional[int] = update.block.block_time.timestamp
            now_perf = time.perf_counter()

            msg = getattr(tx, "message", None)
            if msg is None:
                return None

            parsed_creation_info: Optional[Dict] = None
            parsed_buy_infos: List[Dict] = []
            parsed_sell_infos: List[Dict] = []
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

                # Raw instruction data bytes
                if ix.data.startswith(PumpFunData.CREATE_IX_DISCRIMINATOR) and not parsed_creation_info:
                    tx_signature = Signature(tx.signatures[0])
                    logger.info(f"tx_signature: {tx_signature}")

                    parsed_creation_info = self.event_processor.process_creation_instruction(
                        instruction_data=ix.data,  # Raw instruction data bytes
                        transaction_meta=tx_meta,
                        transaction_resolved_keys=msg.account_keys,  # Full list of Pubkeys for the transaction
                        tx_signature=tx_signature,
                        outer_create_ix_accounts=ix.accounts  # List of u8 indices into account_keys_from_msg
                    )
                    logger.info(f"parsed_creation_info: {parsed_creation_info}")


            processed_creation_obj: Optional[NewTokenTrackedInfo] = None
            # if parsed_creation_info:
            #     dev_initial_buy_lamports = (
            #         parsed_buy_infos[0].get("sol_amount_lamports") if parsed_buy_infos else None
            #     )
            #     if (
            #         parsed_buy_infos
            #         and (buyer := parsed_buy_infos[0].get("buyer_wallet"))
            #         and buyer != parsed_creation_info.get("developer_signer")
            #     ):
            #         parsed_creation_info["developer_signer"] = buyer
            #
            #     try:
            #         required_keys = [
            #             "mint", "developer_signer", "name", "symbol", "decimals",
            #             "bonding_curve", "associated_bonding_curve", "creator_vault",
            #         ]
            #         if all(parsed_creation_info.get(k) is not None for k in required_keys):
            #             initial_buy_sol = (
            #                 dev_initial_buy_lamports / LAMPORTS_PER_SOL
            #                 if dev_initial_buy_lamports is not None else None
            #             )
            #             processed_creation_obj = NewTokenTrackedInfo(
            #                 **parsed_creation_info,
            #                 transaction_signature=tx_signature,
            #                 block_time=tx_block_time,
            #                 dev_initial_buy_sol=initial_buy_sol,
            #                 detection_timestamp=now_perf,
            #             )
            #             mint_pk = processed_creation_obj.mint
            #             self.created_tokens_info[mint_pk] = processed_creation_obj
            #             if mint_pk not in self.tracked_token_activity:
            #                 self.tracked_token_activity[mint_pk] = TokenActivityStats(mint=mint_pk)
            #
            #             logger.info(
            #                 f"TOKEN CREATED: '{processed_creation_obj.symbol}' ({mint_pk}) stored "
            #                 f"with detection_timestamp: {processed_creation_obj.detection_timestamp:.4f}"
            #             )
            #     except Exception as e_crt_obj:
            #         logger.error(f"TX {tx_sig} S{slot} CREATE obj error: {e_crt_obj}.", exc_info=True)

                # elif ix.data.startswith(PumpFunData.BUY_IX_DISCRIMINATOR):
                #     if buy_info := self.event_processor.process_buy_instruction(
                #             **proc_args, outer_buy_ix_accounts=ix.accounts
                #     ):
                #         parsed_buy_infos.append(buy_info)
                # elif ix.data.startswith(PumpFunData.SELL_IX_DISCRIMINATOR):
                #     if sell_info := self.event_processor.process_sell_instruction(
                #             **proc_args, outer_sell_ix_accounts=ix.accounts
                #     ):
                #         parsed_sell_infos.append(sell_info)
                # token_info = self.event_processor.process_transaction_data(
                #     ix.data,  # Raw instruction data bytes
                #     ix.accounts,  # List of u8 indices into account_keys_from_msg
                #     msg.account_keys  # Full list of Pubkeys for the transaction
                # )
                # if token_info:
                #     return token_info
                #
            return None
            
        except Exception as e:
            logger.error(f"Error processing Geyser update: {e}")
            return None
