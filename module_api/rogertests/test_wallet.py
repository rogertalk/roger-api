# -*- coding: utf-8 -*-

from roger import accounts, models
from roger_common import errors
import rogertests


class BaseTestCase(rogertests.JSONServiceTestCase):
    def mint_and_create_tx(self, mint_amount, to_wallet_key, transfer_amount, comment):
        self.bank_counter += 1
        wallet_id = 'bank_wallet_%d' % (self.bank_counter,)
        future = models.Wallet.create_internal_async(self.bank.key, wallet_id,
                                                     mint_amount, 'Free money!')
        mint_wallet = future.get_result()
        future = models.Wallet.create_tx_async(mint_wallet.account, mint_wallet.key,
                                               to_wallet_key, transfer_amount,
                                               comment)
        return mint_wallet, future.get_result()

    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.anna = accounts.create('anna', status='active')
        _, wallet = models.Wallet.create_async(self.anna.key).get_result()
        self.anna_wallet_key = wallet.key
        self.bank = accounts.create('bank', status='active')
        self.bank_counter = 0


class Wallet(BaseTestCase):
    def test_create_account_wallet(self):
        bob = accounts.create('bob', status='active')
        # Confirm that account doesn't have a wallet yet.
        self.assertIsNone(bob.wallet)
        # Create a wallet for the account.
        result = models.Wallet.create_async(bob.key).get_result()
        updated_bob, wallet = result
        self.assertEqual(updated_bob.wallet, wallet.key)
        self.assertEqual(wallet.account, bob.key)
        self.assertEqual(wallet.balance, 0)

    def test_create_and_transfer(self):
        future = models.Wallet.create_and_transfer_async(
            self.anna.key, self.anna_wallet_key,
            'new_wallet_id_123', 123, 'This is a test')
        wallet = future.get_result()
        self.assertEqual(wallet.key, self.anna_wallet_key)
        self.assertEqual(wallet.balance, 123)

    def test_create_internal_wallet(self):
        future = models.Wallet.create_internal_async(self.bank.key, 'test_wallet',
                                                     100, 'Unit test wallet')
        wallet = future.get_result()
        self.assertEqual(wallet.key.id(), 'test_wallet')
        self.assertEqual(wallet.account, self.bank.key)
        self.assertEqual(wallet.balance, 100)
        self.assertEqual(wallet.comment, 'Unit test wallet')
        self.assertEqual(wallet.total_received, 100)
        self.assertEqual(wallet.total_sent, 0)

    def test_transfer(self):
        _, tx = self.mint_and_create_tx(100, self.anna_wallet_key, 13, 'Testing 13')
        # Execute transaction to transfer some coins and verify end result.
        w1, tx1, w2, tx2 = tx().get_result()
        self.assertEqual(w1.balance, 87)
        self.assertEqual(tx1.delta, -13)
        self.assertEqual(w2.balance, 13)
        self.assertEqual(tx2.delta, 13)

    def test_transfer_double(self):
        mint_wallet, tx = self.mint_and_create_tx(100, self.anna_wallet_key, 13, 'Testing 13')
        tx().get_result()
        with self.assertRaises(models.WalletChanged):
            tx().get_result()
        # Check wallet balances.
        self.assertEqual(mint_wallet.key.get().balance, 87)
        self.assertEqual(self.anna_wallet_key.get().balance, 13)

    def test_transfer_too_high(self):
        mint_wallet, tx = self.mint_and_create_tx(100, self.anna_wallet_key, 113, 'Too much!')
        # Ensure that transaction cannot succeed.
        with self.assertRaises(errors.InvalidArgument):
            result = tx().get_result()
        # Check wallets to ensure they haven't changed.
        self.assertEqual(mint_wallet.key.get().balance, 100)
        self.assertEqual(self.anna_wallet_key.get().balance, 0)
