"""
Buy operations for pump.fun tokens.
"""

import struct
from typing import Final

from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey
from spl.token.instructions import create_idempotent_associated_token_account

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

# Discriminator for the buy instruction
EXPECTED_DISCRIMINATOR: Final[bytes] = struct.pack("<Q", 16927863322537952870)


class TokenBuyer(Trader):
    """Handles buying tokens on pump.fun."""

    def __init__(
        self,
        client: SolanaClient,
        wallet: Wallet,
        curve_manager: BondingCurveManager,
        priority_fee_manager: PriorityFeeManager,
        buy_sol_amount: float,
        token_amount: int,
        slippage: float = 0.01,
        max_retries: int = 5,
        fixed_fee: int = 3_000_000,
    ):
        """Initialize token buyer.

        Args:
            client: Solana client for RPC calls
            wallet: Wallet for signing transactions
            curve_manager: Bonding curve manager
            buy_sol_amount: Amount of SOL to spend
            token_amount: Amount of tokens to buy
            slippage: Slippage tolerance (0.01 = 1%)
            max_retries: Maximum number of retry attempts
        """
        self.client = client
        self.wallet = wallet
        self.curve_manager = curve_manager
        self.priority_fee_manager = priority_fee_manager
        self.buy_sol_amount = buy_sol_amount
        self.token_amount = token_amount
        self.slippage = slippage
        self.max_retries = max_retries
        self.fixed_fee = fixed_fee

    async def execute(self, token_info: TokenInfo, *args, **kwargs) -> TradeResult:
        """Execute buy operation.

        Args:
            token_info: Token information

        Returns:
            TradeResult with buy outcome
        """
        try:
            # Convert amount to lamports
            amount_lamports = int(self.buy_sol_amount * LAMPORTS_PER_SOL)

            logger.info(f"EXTREME FAST Mode: Buying {self.token_amount} tokens.")
            # Calculate maximum SOL to spend with slippage
            max_amount_lamports = int(amount_lamports * (1 + self.slippage))

            associated_token_account = self.wallet.get_associated_token_address(
                token_info.mint
            )

            tx_signature = await self._send_buy_transaction(
                token_info,
                associated_token_account,
                self.token_amount,
                max_amount_lamports,
            )

            logger.info(
                f"Buying {self.token_amount:.6f} tokens at max {self.buy_sol_amount:.8f} SOL"
            )

            success = await self.client.confirm_transaction(tx_signature)

            if success:
                logger.info(f"Buy transaction confirmed: {tx_signature}")
                return TradeResult(
                    success=True,
                    tx_signature=tx_signature,
                    amount=token_amount,
                    price=1,
                )
            else:
                return TradeResult(
                    success=False,
                    error_message=f"Transaction failed to confirm: {tx_signature}",
                )

        except Exception as e:
            logger.error(f"Buy operation failed: {e!s}")
            return TradeResult(success=False, error_message=str(e))

    async def _send_buy_transaction(
        self,
        token_info: TokenInfo,
        associated_token_account: Pubkey,
        token_amount: float,
        max_amount_lamports: int,
    ) -> str:
        """Send buy transaction.

        Args:
            token_info: Token information
            associated_token_account: User's token account
            token_amount: Amount of tokens to buy
            max_amount_lamports: Maximum SOL to spend in lamports

        Returns:
            Transaction signature

        Raises:
            Exception: If transaction fails after all retries
        """
        accounts = [
            AccountMeta(pubkey=PumpAddresses.GLOBAL, is_signer=False, is_writable=False),
            AccountMeta(pubkey=PumpAddresses.FEE, is_signer=False, is_writable=True),  # SOL fees
            AccountMeta(pubkey=token_info.mint, is_signer=False, is_writable=False),
            AccountMeta(pubkey=token_info.bonding_curve, is_signer=False, is_writable=True),
            AccountMeta(pubkey=token_info.associated_bonding_curve, is_signer=False, is_writable=True),
            AccountMeta(pubkey=associated_token_account, is_signer=False, is_writable=True),  # User's ATA
            AccountMeta(pubkey=self.wallet.pubkey, is_signer=True, is_writable=True),  # User (signer)
            AccountMeta(pubkey=SystemAddresses.PROGRAM, is_signer=False, is_writable=False),
            AccountMeta(pubkey=SystemAddresses.TOKEN_PROGRAM, is_signer=False, is_writable=False),
            AccountMeta(pubkey=token_info.creator_vault, is_signer=False, is_writable=True),
            AccountMeta(pubkey=PumpAddresses.EVENT_AUTHORITY, is_signer=False, is_writable=False),
            AccountMeta(pubkey=PumpAddresses.PROGRAM, is_signer=False, is_writable=False),
        ]

        # Prepare idempotent create ATA instruction: it will not fail if ATA already exists
        idempotent_ata_ix = create_idempotent_associated_token_account(
            payer=self.wallet.pubkey,
            owner=self.wallet.pubkey,
            mint=token_info.mint,
        )

        # Prepare buy instruction data
        token_amount_raw = int(token_amount * 10**TOKEN_DECIMALS)
        data = (
            EXPECTED_DISCRIMINATOR
            + struct.pack("<Q", token_amount_raw)
            + struct.pack("<Q", max_amount_lamports)
        )
        buy_ix = Instruction(PumpAddresses.PROGRAM, data, accounts)

        logger.info(
            f"BUYER TX PACKING: token_amount_raw: {token_amount_raw}, "
            f"max_amount_lamports: {max_amount_lamports}"
        )

        try:
            return await self.client.build_and_send_transaction(
                [idempotent_ata_ix, buy_ix],
                self.wallet.keypair,
                skip_preflight=True,
                max_retries=self.max_retries,
                # priority_fee=await self.priority_fee_manager.calculate_priority_fee(
                #     self._get_relevant_accounts(token_info)
                # ),
                priority_fee=3_000_000
            )
        except Exception as e:
            logger.error(f"Buy transaction failed: {e!s}")
            raise
