"""
Event processing for pump.fun tokens using Geyser data.
"""

import struct
from typing import Final, Optional, Dict, List, Tuple
from decimal import Decimal
from solders.signature import Signature
from spl.token.instructions import get_associated_token_address


from solders.pubkey import Pubkey

from core.pubkeys import SystemAddresses
from utils.logger import get_logger
from src.geyser.generated.solana_storage_pb2 import TransactionStatusMeta as GrpcTransactionStatusMeta, \
    TokenBalance as GrpcTokenBalance

logger = get_logger(__name__)

PUMP_FUN_DEFAULT_TOKEN_DECIMALS = 6
LAMPORTS_PER_SOL = 1_000_000_000
DEFAULT_MAX_SOL_COST_REALIZATION_FACTOR = 1.0


class PumpFunData:
    PROGRAM_ID = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
    CREATE_IX_DISCRIMINATOR = bytes([24,30,200,40,5,28,7,119])
    SELL_IX_DISCRIMINATOR = bytes([51,230,133,164,1,127,131,173])
    BUY_IX_DISCRIMINATOR = bytes([102,6,61,18,1,218,235,234])
    CREATE_IX_ACCOUNTS_INDICES = {"mint":0,"mint_authority_pda":1,"bonding_curve_pda":2,"assoc_bonding_curve_ata_pda":3,"global_account":4,"mpl_token_metadata_program":5,"metadata_account_pda":6,"user_signer":7,"system_program":8,"token_program":9,"assoc_token_program":10,"rent_program":11,"event_authority_pda":12,"program_itself":13}
    BUY_IX_ACCOUNTS_INDICES = {"global_account":0,"fee_recipient":1,"mint":2,"bonding_curve_pda":3,"assoc_bonding_curve_ata_pda":4,"user_token_account":5,"user_signer":6,"system_program":7,"token_program":8,"creator_vault":9,"event_authority_pda":10,"program_itself":11}
    SELL_IX_ACCOUNTS_INDICES = {"global_account":0,"fee_recipient":1,"mint":2,"bonding_curve_pda":3,"assoc_bonding_curve_ata_pda":4,"user_token_account":5,"user_signer":6,"system_program":7,"assoc_token_program":8,"token_program":9,"event_authority_pda":10,"program_itself":11}



class GeyserEventProcessor:
    """Processes token creation events from Geyser stream."""
    
    def __init__(
            self,
            pump_program: Pubkey,
            max_sol_cost_realization_factor: float = DEFAULT_MAX_SOL_COST_REALIZATION_FACTOR,
            debug_mode: bool = False,
    ):
        """Initialize event processor.

        Args:
            pump_program: Pump.fun program address
            max_sol_cost_realization_factor: Factor to multiply max SOL cost from instruction data
            debug_mode: Whether to log debug information
        """
        self.pump_program = pump_program
        self.max_sol_cost_realization_factor = max_sol_cost_realization_factor
        self.debug_mode = debug_mode
        logger.info(f"GeyserEventProcessor initialized. RealizationFactor: {self.max_sol_cost_realization_factor:.2f}. DebugMode: {self.debug_mode}")

    def process_transaction_data(self, instruction_data: bytes, accounts: list, keys: list) -> TokenInfo | None:
        """Process transaction data and extract token creation info.

        Args:
            instruction_data: Raw instruction data
            accounts: List of account indices
            keys: List of account public keys

        Returns:
            TokenInfo if token creation found, None otherwise
        """
        if not instruction_data.startswith(PumpFunData.CREATE_DISCRIMINATOR):
            return None

        try:
            # Skip past the 8-byte discriminator
            offset = 8

            # Helper to read strings (prefixed with length)
            def read_string():
                nonlocal offset
                # Get string length (4-byte uint)
                length = struct.unpack_from("<I", instruction_data, offset)[0]
                offset += 4
                # Extract and decode the string
                value = instruction_data[offset:offset + length].decode("utf-8")
                offset += length
                return value

            def read_pubkey():
                nonlocal offset
                value = base58.b58encode(instruction_data[offset: offset + 32]).decode("utf-8")
                offset += 32
                return Pubkey.from_string(value)

            # Helper to get account key
            def get_account_key(index):
                if index >= len(accounts):
                    return None
                account_index = accounts[index]
                if account_index >= len(keys):
                    return None
                return Pubkey.from_bytes(keys[account_index])

            name = read_string()
            symbol = read_string()
            uri = read_string()
            creator = read_pubkey()

            mint = get_account_key(0)
            bonding_curve = get_account_key(2)
            associated_bonding_curve = get_account_key(3)
            user = get_account_key(7)

            creator_vault = self._find_creator_vault(creator)

            if not all([mint, bonding_curve, associated_bonding_curve, user]):
                logger.warning("Missing required account keys in token creation")
                return None

            return TokenInfo(
                name=name,
                symbol=symbol,
                uri=uri,
                mint=mint,
                bonding_curve=bonding_curve,
                associated_bonding_curve=associated_bonding_curve,
                user=user,
                creator=creator,
                creator_vault=creator_vault,
            )

        except Exception as e:
            logger.error(f"Failed to process transaction data: {e}")
            return None

    def _parse_string_from_ix_data(
            self,
            data: bytes,
            offset: int,
            tx_sig: Signature,
            field_name: str = "string",
    ) -> Tuple[Optional[str], int]:
        """
        Parse a string from instruction data.

        Args:
            data: Instruction data
            offset: Offset to start parsing
            tx_sig: Transaction signature
            field_name: Name of the field to parse

        Returns:
            Tuple containing the parsed string and the new offset

        Raises:
            Exception: If there is an error parsing the string
        """
        try:
            length = struct.unpack_from("<I", data, offset)[0];
            offset += 4
            if length == 0:
                return "", offset
            if offset + length > len(data):
                logger.warning(f"TX {tx_sig}: _parse_string '{field_name}' len {length} OOB.")
                return None, len(data)
            value = data[offset:offset + length].decode("utf-8", errors="replace");
            return value, offset + length
        except Exception as e:
            logger.error(f"TX {tx_sig}: _parse_string '{field_name}' unexpected error: {e}",
                         exc_info=self.debug_mode)
            return None, len(data)

    def _get_token_balance_from_pb(
        self,
        token_balance_pb: GrpcTokenBalance,
        resolved_txn_keys: List[Pubkey],
        tx_sig: Signature,
        bal_type: str,
    ) -> Optional[Dict]:
        """
        Get token balance from token balance protobuf.

        Args:
            token_balance_pb: Token balance protobuf
            resolved_txn_keys: List of resolved transaction keys
            tx_sig: Transaction signature
            bal_type: Type of balance to get

        Returns:
            Dictionary containing the token balance

        Raises:
            Exception: If there is an error parsing the token balance
        """
        try:
            acc_idx = token_balance_pb.account_index;
            token_account_pk = resolved_txn_keys[acc_idx]  # type: ignore
            mint_str = token_balance_pb.mint  # type: ignore
            raw_a = token_balance_pb.ui_token_amount.amount if hasattr(token_balance_pb,
                                                                       'ui_token_amount') else None  # type: ignore
            dec_v = token_balance_pb.ui_token_amount.decimals if hasattr(token_balance_pb,
                                                                         'ui_token_amount') else None  # type: ignore
            if raw_a is None or dec_v is None: return None
            return {
                "token_account": token_account_pk,
                "mint": Pubkey.from_string(mint_str),
                "raw_amount": int(raw_a),
                "decimals": int(dec_v),
            }
        except Exception as e:
            logger.error(f"TX {tx_sig}: Parse {bal_type}_token_bal error: {e}", exc_info=self.debug_mode)
            return None

    def process_creation_instruction(
        self,
        instruction_data: bytes,
        outer_create_ix_accounts: List[Pubkey],
        transaction_meta: Optional[GrpcTransactionStatusMeta],
        transaction_resolved_keys: List[Pubkey],
        tx_signature: Signature,
    ) -> Optional[Dict]:
        """
        Process a creation instruction from the pump.fun program.
        """
        if not instruction_data.startswith(PumpFunData.CREATE_IX_DISCRIMINATOR):
            return None

        try:
            indices = PumpFunData.CREATE_IX_ACCOUNTS_INDICES
            mint_pk = outer_create_ix_accounts[indices["mint"]]
            creator_pk = outer_create_ix_accounts[indices["user_signer"]]
            offset = len(PumpFunData.CREATE_IX_DISCRIMINATOR)
            name, offset = self._parse_string_from_ix_data(instruction_data, offset, tx_signature, "name")
            symbol, offset = self._parse_string_from_ix_data(instruction_data, offset, tx_signature, "symbol")
            uri, _ = self._parse_string_from_ix_data(instruction_data, offset, tx_signature, "uri")
            creator_vault_pda, _ = Pubkey.find_program_address(
                [b"creator-vault", bytes(creator_pk)],
                PumpFunData.PROGRAM_ID,
            )
            res = {
                "mint": mint_pk,
                "developer_signer": creator_pk,
                "name": name or "",
                "symbol": symbol or "",
                "uri": uri,
                "bonding_curve": outer_create_ix_accounts[indices["bonding_curve_pda"]],
                "associated_bonding_curve": outer_create_ix_accounts[indices["assoc_bonding_curve_ata_pda"]],
                "creator_arg": creator_pk,
                "decimals": PUMP_FUN_DEFAULT_TOKEN_DECIMALS,
                "developer_token_account_after_creation": get_associated_token_address(creator_pk, mint_pk),
                "creator_vault": creator_vault_pda,
            }
            if transaction_meta and hasattr(transaction_meta, 'post_token_balances'):
                for tb in transaction_meta.post_token_balances:
                    if (
                        parsed := self._get_token_balance_from_pb(
                            tb, transaction_resolved_keys, tx_signature, "post_create"
                        )
                    ) and parsed["token_account"] == res["developer_token_account_after_creation"]:
                        res["dev_tokens_from_creation_event_raw"] = parsed["raw_amount"]
                        res["decimals"] = parsed["decimals"]
                        break

            logger.info(
                f"CREATE Event [TX: {str(tx_signature)[:10]}..]: Mint={res['mint']}, Sym='{res['symbol']}', Dev={res['developer_signer']}")
            return res
        except Exception as e:
            logger.error(f"TX {tx_signature}: CREATE Error in processor: {e}", exc_info=self.debug_mode); return None

    def process_sell_instruction(
        self,
        instruction_data: bytes,
        outer_sell_ix_accounts: List[Pubkey],
        transaction_meta: Optional[GrpcTransactionStatusMeta],
        transaction_resolved_keys: List[Pubkey],
        tx_signature: Signature,
        transaction_static_keys: List[Pubkey],
        num_transaction_signers: int,
    ) -> Optional[Dict]:
        """Process a sell instruction from the pump.fun program.
        
        Args:
            instruction_data: Raw instruction data
            outer_sell_ix_accounts: List of account public keys involved in the sell instruction
            transaction_meta: Transaction metadata containing balances and token info
            transaction_resolved_keys: List of resolved account public keys
            tx_signature: Transaction signature
            transaction_static_keys: List of static account public keys
            num_transaction_signers: Number of transaction signers
            
        Returns:
            Dictionary containing sell event details or None if processing fails
        """
        if not instruction_data.startswith(PumpFunData.SELL_IX_DISCRIMINATOR):
            return None

        try:
            # Extract relevant account public keys
            indices = PumpFunData.SELL_IX_ACCOUNTS_INDICES
            mint_pk = outer_sell_ix_accounts[indices["mint"]]
            seller_pk = outer_sell_ix_accounts[indices["user_signer"]]
            seller_ata_pk = outer_sell_ix_accounts[indices["user_token_account"]]

            # Initialize variables for tracking token amounts and balances
            sold_amount_raw = None
            remaining_balance_raw = None
            sol_received = None
            token_decimals = PUMP_FUN_DEFAULT_TOKEN_DECIMALS

            # Process transaction metadata if available
            if transaction_meta:
                # Get token balances before and after the sell
                pre_balance_ata = None
                post_balance_ata = None

                if hasattr(transaction_meta, 'pre_token_balances'):
                    for balance in transaction_meta.pre_token_balances:
                        if (parsed := self._get_token_balance_from_pb(balance, transaction_resolved_keys, 
                                                                    tx_signature, "pre_sell")) and \
                           parsed["token_account"] == seller_ata_pk:
                            pre_balance_ata = parsed["raw_amount"]
                            token_decimals = parsed["decimals"]
                            break

                if hasattr(transaction_meta, 'post_token_balances'):
                    for balance in transaction_meta.post_token_balances:
                        if (parsed := self._get_token_balance_from_pb(balance, transaction_resolved_keys, 
                                                                    tx_signature, "post_sell")) and \
                           parsed["token_account"] == seller_ata_pk:
                            post_balance_ata = parsed["raw_amount"]
                            token_decimals = parsed["decimals"]
                            break

                # Calculate amounts sold and remaining
                if pre_balance_ata is not None and post_balance_ata is not None:
                    sold_amount_raw = pre_balance_ata - post_balance_ata
                    remaining_balance_raw = post_balance_ata

                # Calculate SOL received
                if hasattr(transaction_meta, 'pre_balances') and hasattr(transaction_meta, 'post_balances'):
                    seller_idx = next((i for i, k in enumerate(transaction_resolved_keys) if k == seller_pk), -1)
                    if seller_idx != -1:
                        fee = transaction_meta.fee if transaction_static_keys and transaction_static_keys[0] == seller_pk else 0
                        sol_received = (transaction_meta.post_balances[seller_idx] - 
                                      transaction_meta.pre_balances[seller_idx]) + fee

            # Fallback to instruction data if metadata processing failed
            if sold_amount_raw is None and len(instruction_data) >= 16:
                sold_amount_raw = struct.unpack_from("<Q", instruction_data, 8)[0]

            if sold_amount_raw is None:
                return None

            # Prepare result dictionary
            result = {
                "mint": mint_pk,
                "seller_wallet": seller_pk,
                "seller_token_account": seller_ata_pk,
                "amount_sold_ui": float(Decimal(str(sold_amount_raw)) / (Decimal(10)**token_decimals)),
                "amount_sold_raw": sold_amount_raw,
                "decimals": token_decimals,
                "is_full_sell": remaining_balance_raw == 0,
                "remaining_balance_raw": remaining_balance_raw,
                "sol_received_lamports": sol_received,
                "num_signers": num_transaction_signers
            }

            logger.info(f"SELL Event [TX: {str(tx_signature)[:10]}..]: "
                       f"Mint={result['mint']}, Amount={result['amount_sold_ui']:.4f}, "
                       f"Seller={result['seller_wallet']}, Signers={num_transaction_signers}")
            return result

        except Exception as e:
            logger.error(f"TX {tx_signature} Mint {mint_pk if 'mint_pk' in locals() else 'N/A'}: "
                        f"SELL - Unexpected: {e}", exc_info=self.debug_mode)
            return None

    def process_buy_instruction(
        self, instruction_data: bytes, outer_buy_ix_accounts: List[Pubkey],
        transaction_meta: Optional[GrpcTransactionStatusMeta],
        transaction_resolved_keys: List[Pubkey],
        tx_signature: Signature,
        transaction_static_keys: List[Pubkey],
        num_transaction_signers: int,
        outer_instruction_index: int,
    ) -> Optional[Dict]:
        """Process a buy instruction from the pump.fun program.
        
        Args:
            instruction_data: Raw instruction data
            outer_buy_ix_accounts: List of account public keys involved in the buy instruction
            transaction_meta: Transaction metadata containing balances and token info
            transaction_resolved_keys: List of resolved account public keys
            tx_signature: Transaction signature
            transaction_static_keys: List of static account public keys
            num_transaction_signers: Number of transaction signers
            outer_instruction_index: Index of the outer instruction
            
        Returns:
            Dictionary containing buy event details or None if processing fails
        """
        if not instruction_data.startswith(PumpFunData.BUY_IX_DISCRIMINATOR):
            return None

        try:
            # Extract relevant account public keys
            indices = PumpFunData.BUY_IX_ACCOUNTS_INDICES
            mint_pk = outer_buy_ix_accounts[indices["mint"]]
            buyer_pk = outer_buy_ix_accounts[indices["user_signer"]]
            bonding_curve_pk = outer_buy_ix_accounts[indices["bonding_curve_pda"]]

            # Get buyer's token account
            user_token_account_idx = indices.get("user_token_account")
            buyer_dest_ata = (outer_buy_ix_accounts[user_token_account_idx] 
                            if user_token_account_idx is not None and user_token_account_idx < len(outer_buy_ix_accounts)
                            else get_associated_token_address(buyer_pk, mint_pk))

            # Initialize variables for tracking amounts
            sol_spent_from_balance = None
            tokens_received = None
            token_decimals = PUMP_FUN_DEFAULT_TOKEN_DECIMALS
            sol_sent_to_bonding_curve = None

            # Get max SOL cost from instruction data
            max_sol_cost_ix = (struct.unpack_from("<Q", instruction_data, 16)[0] 
                             if len(instruction_data) >= 24 else None)

            # Process transaction metadata if available
            if transaction_meta:
                # Look for SOL transfer to bonding curve in inner instructions
                if hasattr(transaction_meta, 'inner_instructions'):
                    for inner_ix_set in transaction_meta.inner_instructions:
                        if inner_ix_set.index == outer_instruction_index:
                            for inner_ix in inner_ix_set.instructions:
                                prog_id = transaction_static_keys[inner_ix.program_id_index]
                                if (prog_id == SystemAddresses.PROGRAM and
                                    inner_ix.data.startswith(b'\x02\x00\x00\x00')):
                                    inner_accounts_indices = list(inner_ix.accounts)
                                    if len(inner_accounts_indices) >= 2:
                                        source_pk = transaction_resolved_keys[inner_accounts_indices[0]]
                                        dest_pk = transaction_resolved_keys[inner_accounts_indices[1]]
                                        if source_pk == buyer_pk and dest_pk == bonding_curve_pk:
                                            sol_sent_to_bonding_curve = struct.unpack_from("<Q", 
                                                                                         inner_ix.data, 4)[0]
                                            break
                            if sol_sent_to_bonding_curve is not None:
                                break

                # Calculate SOL spent if not found in inner instructions
                if sol_sent_to_bonding_curve is None:
                    if hasattr(transaction_meta, 'pre_balances') and hasattr(transaction_meta, 'post_balances'):
                        buyer_idx = next((i for i, k in enumerate(transaction_resolved_keys) 
                                        if k == buyer_pk), -1)
                        if buyer_idx != -1:
                            fee = (transaction_meta.fee 
                                  if transaction_static_keys and transaction_static_keys[0] == buyer_pk 
                                  else 0)
                            sol_spent_from_balance = (transaction_meta.pre_balances[buyer_idx] - 
                                                    transaction_meta.post_balances[buyer_idx]) - fee

                # Calculate tokens received
                pre_bal_ata, post_bal_ata = 0, 0
                if hasattr(transaction_meta, 'pre_token_balances'):
                    for balance in transaction_meta.pre_token_balances:
                        if (parsed := self._get_token_balance_from_pb(balance, transaction_resolved_keys, 
                                                                    tx_signature, "pre_buy")) and \
                           parsed["token_account"] == buyer_dest_ata:
                            pre_bal_ata = parsed["raw_amount"]
                            token_decimals = parsed["decimals"]
                            break

                if hasattr(transaction_meta, 'post_token_balances'):
                    for balance in transaction_meta.post_token_balances:
                        if (parsed := self._get_token_balance_from_pb(balance, transaction_resolved_keys, 
                                                                    tx_signature, "post_buy")) and \
                           parsed["token_account"] == buyer_dest_ata:
                            post_bal_ata = parsed["raw_amount"]
                            token_decimals = parsed["decimals"]
                            break

                tokens_received = post_bal_ata - pre_bal_ata

            # Determine final SOL amount
            sol_amount_lamports = (
                sol_sent_to_bonding_curve if sol_sent_to_bonding_curve is not None
                else (sol_spent_from_balance if sol_spent_from_balance is not None
                      else (int(max_sol_cost_ix * self.max_sol_cost_realization_factor) 
                            if max_sol_cost_ix is not None else 0))
            )

            # Prepare result dictionary
            result = {
                "mint": mint_pk,
                "buyer_wallet": buyer_pk,
                "buyer_token_account": buyer_dest_ata,
                "sol_amount_lamports": sol_amount_lamports,
                "tokens_received_raw": tokens_received,
                "tokens_received_ui": (float(Decimal(str(tokens_received)) / (Decimal(10)**token_decimals)) 
                                     if tokens_received is not None else 0.0),
                "decimals": token_decimals,
                "max_sol_cost_from_ix": max_sol_cost_ix,
                "num_signers": num_transaction_signers
            }

            sol_ui = result['sol_amount_lamports'] / LAMPORTS_PER_SOL if result['sol_amount_lamports'] is not None else 'N/A'
            logger.info(f"BUY Event [TX: {str(tx_signature)[:10]}..]: "
                       f"Mint={result['mint']}, SOL={sol_ui:.4f}, "
                       f"Buyer={result['buyer_wallet']}, Signers={num_transaction_signers}")
            return result

        except Exception as e:
            logger.error(f"TX {tx_signature} Mint {mint_pk if 'mint_pk' in locals() else 'N/A'}: "
                        f"BUY - Unexpected error: {e}", exc_info=self.debug_mode)
            return None