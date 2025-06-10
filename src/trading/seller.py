"""
Sell operations for pump.fun tokens.
"""

import struct
from typing import Final

from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey

from core.client import SolanaClient
from core.curve import BondingCurveManager
from core.priority_fee.manager import PriorityFeeManager
from core.pubkeys import (
    LAMPORTS_PER_SOL,
    TOKEN_DECIMALS,
    PumpAddresses,
    SystemAddresses,
)
from core.wallet import Wallet
from trading.base import TokenInfo, Trader, TradeResult
from utils.logger import get_logger

logger = get_logger(__name__)

# Discriminator for the sell instruction
EXPECTED_DISCRIMINATOR: Final[bytes] = struct.pack("<Q", 12502976635542562355)


class TokenSeller(Trader):
    """Handles selling tokens on pump.fun."""

    def __init__(
        self,
        client: SolanaClient,
        wallet: Wallet,
        curve_manager: BondingCurveManager,
        priority_fee_manager: PriorityFeeManager,
        token_balance: int,
        slippage: float = 0.25,
        max_retries: int = 5,
    ):
        """Initialize token seller.

        Args:
            client: Solana client for RPC calls
            wallet: Wallet for signing transactions
            curve_manager: Bonding curve manager
            slippage: Slippage tolerance (0.25 = 25%)
            max_retries: Maximum number of retry attempts
        """
        self.client = client
        self.wallet = wallet
        self.curve_manager = curve_manager
        self.priority_fee_manager = priority_fee_manager
        self.slippage = slippage
        self.max_retries = max_retries
        self.token_balance = token_balance

    async def execute(self, token_info: TokenInfo, *args, **kwargs) -> TradeResult:
        """Execute sell operation.

        Args:
            token_info: Token information

        Returns:
            TradeResult with sell outcome
        """
        try:
            # Get associated token account
            associated_token_account = self.wallet.get_associated_token_address(
                token_info.mint
            )

            logger.info(f"Token balance: {self.token_balance}")
            if self.token_balance == 0:
                logger.info("No tokens to sell.")
                return TradeResult(success=False, error_message="No tokens to sell")

            # Calculate minimum SOL output with slippage
            min_sol_output = 1  # Always 1 lamport for aggressive sell

            logger.info(f"Selling {self.token_balance} tokens")
            # logger.info(
            #     f"Minimum SOL output (with {self.slippage * 100}% slippage): {min_sol_output / LAMPORTS_PER_SOL:.8f} SOL"
            # )

            tx_signature = await self._send_sell_transaction(
                token_info,
                associated_token_account,
                self.token_balance,
                min_sol_output,
            )

            success = await self.client.confirm_transaction(tx_signature)
            if success:
                logger.info(f"Sell transaction confirmed: {tx_signature}")
                return TradeResult(
                    success=True,
                    tx_signature=tx_signature,
                    amount=self.token_balance,
                    price=1,
                )
            else:
                return TradeResult(
                    success=False,
                    error_message=f"Transaction failed to confirm: {tx_signature}",
                )

        except Exception as e:
            logger.error(f"Sell operation failed: {e!s}")
            return TradeResult(success=False, error_message=str(e))

    async def _send_sell_transaction(
        self,
        token_info: TokenInfo,
        associated_token_account: Pubkey,
        token_amount: int,
        min_sol_output: int,
    ) -> str:
        """Send sell transaction.

        Args:
            mint: Token information
            associated_token_account: User's token account
            token_amount: Amount of tokens to sell in raw units
            min_sol_output: Minimum SOL to receive in lamports

        Returns:
            Transaction signature

        Raises:
            Exception: If transaction fails after all retries
        """
        # Prepare sell instruction accounts
        accounts = [
            AccountMeta(pubkey=PumpAddresses.GLOBAL, is_signer=False, is_writable=False),
            AccountMeta(pubkey=PumpAddresses.FEE, is_signer=False, is_writable=True),  # SOL fees
            AccountMeta(pubkey=token_info.mint, is_signer=False, is_writable=False),
            AccountMeta(pubkey=token_info.bonding_curve, is_signer=False, is_writable=True),
            AccountMeta(pubkey=token_info.associated_bonding_curve, is_signer=False, is_writable=True),
            AccountMeta(pubkey=associated_token_account, is_signer=False, is_writable=True),  # User's ATA
            AccountMeta(pubkey=self.wallet.pubkey, is_signer=True, is_writable=True),  # User (signer)
            AccountMeta(pubkey=SystemAddresses.PROGRAM, is_signer=False, is_writable=False),
            AccountMeta(pubkey=token_info.creator_vault, is_signer=False, is_writable=True),
            # Assuming creator_vault in TokenInfo
            AccountMeta(pubkey=SystemAddresses.TOKEN_PROGRAM, is_signer=False, is_writable=False),
            AccountMeta(pubkey=PumpAddresses.EVENT_AUTHORITY, is_signer=False, is_writable=False),
            AccountMeta(pubkey=PumpAddresses.PROGRAM, is_signer=False, is_writable=False),
        ]

        # Prepare sell instruction data
        data = (
            EXPECTED_DISCRIMINATOR
            + struct.pack("<Q", token_amount)
            + struct.pack("<Q", min_sol_output)
        )
        sell_ix = Instruction(PumpAddresses.PROGRAM, data, accounts)

        try:
            return await self.client.build_and_send_transaction(
                instructions=[sell_ix],
                signer_keypair=self.wallet.keypair,
                skip_preflight=True,
                max_retries=self.max_retries,
                # priority_fee=await self.priority_fee_manager.calculate_priority_fee(
                #     self._get_relevant_accounts(token_info)
                # ),
                priority_fee=3_000_000,
                compute_unit_limit=75_000
            )
        except Exception as e:
            logger.error(f"Sell transaction failed: {e!s}")
            raise
