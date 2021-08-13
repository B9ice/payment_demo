"""
Created on Aug 7, 2021

@author: samo
"""

import unittest
from main.payment_gateway import *
import tempfile
import io
import csv


class Test(unittest.TestCase):

    def setUp(self):
        client_accounts_path = pathlib.Path(__name__).resolve().parent / 'client_accounts.csv'
        transactions_path = pathlib.Path(__name__).resolve().parent / 'transactions.csv'
        # flush file
        open(client_accounts_path, 'w').close()
        open(transactions_path, 'w').close()

        self.pm_args = dict(client_csv=client_accounts_path, transaction_csv=transactions_path)

    def tearDown(self):
        pass

    @staticmethod
    def get_csv_params(text, index):
        td = csv.DictReader(io.StringIO(text), fieldnames=PaymentManager.COLS[index]['fields'])
        next(td)  # skip header line
        args = [d.items() for d in td]
        kwargs = []
        for i in args:
            kwargs.append({k.strip(): str(v).strip() for k, v in i})
        return kwargs

    def test_deposit_for_new_client_without_id(self):
        td = dict(type="deposit", client="", tx="001", amount="10.00")

        pm = process(td, **self.pm_args)
        transactions = pm.get_record('tx',  False, '001')
        cx = transactions['001'][0]['client']
        client = pm.get_record('client', True, cx)

        assert client[cx][0]['available'] == "10.00", "New client not created"

    def test_deposit_for_existing_client(self):
        t = """type,  client,    tx,      amount
          deposit,     001,       001,     10.00
          deposit,     001,       002,     10.00
          """

        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="20.00", total="20.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="10.00", type="deposit", client="001"))
        expected_t['002'].append(dict(tx="002", amount="10.00", type="deposit", client="001"))

        pm = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001', '002')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_duplicate_deposit_for_existing_client(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     10.00
               deposit,     001,       001,     10.00"""

        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="10.00", total="10.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="10.00", type="deposit", client="001"))

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)

        self.assertIsInstance(e, TransactionIDAlreadyExists)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001', '002')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_invalid_client_id_u16_too_large(self):

        cid = str(PaymentManager.MAX_UINT16) + '100'
        td = dict(type="deposit", client=str(cid), tx="001", amount="10.00")
        with self.assertRaises(ValueError):
            process(td, **self.pm_args)

    def test_invalid_tx_id_u32_too_large(self):

        cid = str(PaymentManager.MAX_UINT32) + '100'
        td = dict(type="deposit", client=str(cid), tx="001", amount="10.00")
        with self.assertRaises(ValueError):
            process(td, **self.pm_args)

    def test_invalid_tx_id_u32_non_integer(self):

        cid = str(PaymentManager.MAX_UINT32) + '100FO00'
        td = dict(type="deposit", client=str(cid), tx="001", amount="10.00")
        with self.assertRaises(ValueError):
            process(td, **self.pm_args)


    def test_negative_deposit(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     10.00
               deposit,     001,       002,     -10.00
            """
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="10.00", total="10.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="10.00", type="deposit", client="001"))

        pm,e= process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', True, '001')

        self.assertIsInstance(e, PaymentError)
        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_same_tx_deposit_for_multiple_clients(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     10.00
               deposit,     002,       001,     11.00
               deposit,     002,       001,     15.00
            """

        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="10.00", total="10.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="10.00", type="deposit", client="001"))

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, PaymentError)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', True, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_deposit_locked_account(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     10.00
               """
        c = """client,  held,    available, total,  locked
                   001,     0.00,    0.00,      0.00,    True
               """

        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="0.00", total="0.00", locked="True"))
        expected_t = defaultdict(list)

        # add client client_accounts
        kw = self.get_csv_params(c.strip(), 'client')
        PaymentManager(**self.pm_args).new_client('001', **kw[0]).save_client_accounts()

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, ClientAccountLocked)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', True, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_withdrawal_for_non_existent_client(self):
        t = """type,       client,     tx,      amount
               withdrawal,     001,       001,     10.00
            """
        expected_c = defaultdict(list)
        expected_t = defaultdict(list)

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, ClientNotFound)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', True, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_withdrawal_for_existing_client(self):
        t = """type,       client,     tx,      amount
               withdrawal,     001,       002,     10.00
            """
        c = """client,  held,    available, total,  locked
                001,     0.00,    10.00,      10.00,    False
                       """
        # add client client_accounts
        kw = self.get_csv_params(c.strip(), 'client')
        PaymentManager(**self.pm_args).new_client('001', **kw[0]).save_client_accounts()

        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="0.00", total="0.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['002'].append(dict(tx="002", amount="10.00", type="withdrawal", client="001"))

        pm = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', True, '002')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_negative_withdrawal(self):
        t = """type,       client,     tx,      amount
               deposit,     001,         001,     10.00
               withdrawal,     001,       002,     -10.00
            """
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="10.00", total="10.00", locked="False"))
        expected_t = defaultdict(list)

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', True, '002')

        assert e, "Expected {}".format(WithdrawalError)

        self.assertIsInstance(e, PaymentError)
        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_same_tx_withdrawal_for_multiple_clients(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     10.00
               deposit,     002,       002,     15.00
               withdrawal,     001,       001,     10.00
               withdrawal,     002,       001,     10.00
            """

        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="0.00", total="0.00", locked="False"))
        expected_c['002'].append(dict(client="002", held="0.00", available="15.00", total="15.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="10.00", type="deposit", client="001"))
        expected_t['001'].append(dict(tx="001", amount="10.00", type="withdrawal", client="001"))
        expected_t['002'].append(dict(tx="002", amount="15.00", type="deposit", client="002"))

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, TransactionIDAlreadyExists)

        clients = pm.get_record('client', True, '001', '002')
        transactions = pm.get_record('tx', True, '001', '002')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_insufficient_funds(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     10.00
               withdrawal,     001,       002,     15.00
            """
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="10.00", total="10.00", locked="False"))

        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="10.00", type="deposit", client="001"))

        expected_insuff = defaultdict(list)

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, WithdrawalError)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', True, '001')
        insuff = pm.get_record('tx', True, '002')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)
        self.assertDictEqual(insuff, expected_insuff)

    def test_withdrawal_locked_account(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     10.00
               withdrawal,     001,       002,     15.00
            """
        c = """client,  held,    available, total,  locked
                001,     0.00,    0.00,      0.00,    True
            """

        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="0.00", total="0.00", locked="True"))
        expected_t = defaultdict(list)

        # add client client_accounts
        kw = self.get_csv_params(c.strip(), 'client')
        PaymentManager(**self.pm_args).new_client('001', **kw[0]).save_client_accounts()

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, ClientAccountLocked)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', True, '001', '002')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_valid_dispute(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     20.00
               dispute,     001,       001,      
            """
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="20.00", available="0.00", total="20.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="20.00", type="deposit", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="dispute", client="001"))

        pm = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_valid_chargeback(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     20.00
               dispute,     001,       001,     
               resolve,     001,       001,     
               chargeback,   001,       001,    
            """
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="0.00", total="0.00", locked="True"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="20.00", type="deposit", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="dispute", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="resolve", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="chargeback", client="001"))

        pm = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_withdrawal_after_chargeback(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     20.00
               deposit,     001,       002,     10.00
               dispute,     001,       002,     
               resolve,     001,       002,     
               chargeback,   001,       002, 
               withdrawal,  001,       003,     20.00   
            """
        self.maxDiff = None
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="20.00", total="20.00", locked="True"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="20.00", type="deposit", client="001"))
        expected_t['002'].append(dict(tx="002", amount="10.00", type="deposit", client="001"))
        expected_t['002'].append(dict(tx="002", amount="", type="dispute", client="001"))
        expected_t['002'].append(dict(tx="002", amount="", type="resolve", client="001"))
        expected_t['002'].append(dict(tx="002", amount="", type="chargeback", client="001"))

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, ClientAccountLocked)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001', '002')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_undisputed_chargeback(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     20.00 
               chargeback,   001,       001, 
            """
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="20.00", total="20.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="20.00", type="deposit", client="001"))

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, DisputeError)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_unresolved_chargeback(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     20.00 
               dispute,    001,       001,
               chargeback,   001,       001, 
            """
        self.maxDiff = None
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="20.00", available="0.00", total="20.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="20.00", type="deposit", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="dispute", client="001"))

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, ResolveError)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_resolve_without_dispute(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     20.00 
               resolve,    001,       001,
               chargeback,   001,       001, 
            """
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="20.00", total="20.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="20.00", type="deposit", client="001"))

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, DisputeError)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_duplicate_chargeback(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     20.00 
               dispute,    001,       001,
               resolve,    001,       001,
               chargeback,   001,       001, 
               chargeback,   001,       001, 
            """
        self.maxDiff = None
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="0.00", total="0.00", locked="True"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="20.00", type="deposit", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="dispute", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="resolve", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="chargeback", client="001"))

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, ClientAccountLocked)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_duplicate_despute(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     20.00 
               dispute,    001,       001,
               dispute,    001,       001,
               resolve,    001,       001,
            """
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="20.00", available="0.00", total="20.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="20.00", type="deposit", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="dispute", client="001"))

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, TransactionIDAlreadyExists)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_duplicate_resolve(self):
        t = """type,       client,     tx,      amount
               deposit,     001,       001,     20.00 
               dispute,    001,       001,
               resolve,    001,       001,
               resolve,    001,       001,
            """
        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="20.00", total="20.00", locked="False"))
        expected_t = defaultdict(list)
        expected_t['001'].append(dict(tx="001", amount="20.00", type="deposit", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="dispute", client="001"))
        expected_t['001'].append(dict(tx="001", amount="", type="resolve", client="001"))

        pm, e = process(*self.get_csv_params(t.strip(), 'tx'), **self.pm_args)
        self.assertIsInstance(e, TransactionIDAlreadyExists)

        clients = pm.get_record('client', True, '001')
        transactions = pm.get_record('tx', False, '001')

        self.assertDictEqual(clients, expected_c)
        self.assertDictEqual(transactions, expected_t)

    def test_invalid_tx_encoding(self):
        d = """type,       client,     tx,      amount
               deposit,     001,       001,     20.00
               withdrawal,     001,       002,     20.00  
            """
        utf32 = tempfile.mkstemp(suffix='.csv')
        utf8 = tempfile.mkstemp(suffix='.csv')
        utf16 = tempfile.mkstemp(suffix='.csv')
        with open(utf32[1], "w", encoding="utf32") as w:
            w.write(d)
        with open(utf8[1], "w", encoding="utf8") as w:
            w.write(d)
        with open(utf16[1], "w", encoding="utf16") as w:
            w.write(d)

        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="0.00", total="0.00", locked="False"))
        with self.assertRaises(UnicodeError):
            process(*self.get_csv_params(d.strip(), 'tx'), client_csv=utf8[1], transaction_csv=utf16[1])

        os.remove(utf32[1])
        os.remove(utf8[1])
        os.remove(utf16[1])

    def test_valid_client_encoding(self):
        d = """type,       client,     tx,      amount
                       deposit,     001,       001,     20.00
                       withdrawal,     001,       002,     20.00  
                    """

        c = """
        client,  held,    available, total,  locked
        001,     0.00,    0.00,      0.00,    False
        002,     0.00,    0.00,      0.00,    False
        """

        utf32 = tempfile.mkstemp(suffix='.csv')
        utf16 = tempfile.mkstemp(suffix='.csv')
        with open(utf32[1], "w", encoding="utf32") as w:
            w.write(d)
        with open(utf16[1], "w", encoding="utf16") as w:
            w.write(c)

        expected_c = defaultdict(list)
        expected_c['001'].append(dict(client="001", held="0.00", available="0.00", total="0.00", locked="False"))

        # add client client_accounts
        PaymentManager(client_csv=utf16[1], transaction_csv=utf32[1]).new_client('001', '002').save_client_accounts

        pm = PaymentManager(client_csv=utf16[1], transaction_csv=utf32[1])

        clients = pm.get_record('client', True, '001')

        os.remove(utf16[1])
        os.remove(utf32[1])

        self.assertDictEqual(expected_c, clients)
