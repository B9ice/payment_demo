import copy
import os
import pathlib
import pprint
import struct
import sys
import csv
from collections import defaultdict
from random import randrange
from tempfile import NamedTemporaryFile
import shutil
from decimal import Decimal
import ast


def add(a, b) -> str:
    a: str
    b: str

    a = Decimal(a)
    b = Decimal(b)

    return str(a + b)


def equal_csv_row_dict(a, b) -> bool:
    """
    Strip white space and compare keys and values
    """
    count = 0
    for i, j in a.items():

        for m, n in b.items():
            if str(i).strip() == str(m).strip() and str(j).strip() == str(n).strip():
                count += 1
                break
    return count == len(a) == len(b)


def subtract(a, b) -> str:
    a: str
    b: str

    a = Decimal(a)
    b = Decimal(b)

    return str(a - b)


def encode_file(f, encoding):
    """
    Handy function for encoding a file for testing
    """
    f: str
    encoding: str

    with open(f, "r") as w:
        lines = w.read()
    with open(f, "w", encoding=encoding) as w:
        w.write(lines)


class PaymentError(Exception):
    """
    Custom exception class helps with error handling
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.client_id = kwargs.get('client_id')
        self.transaction_id = kwargs.get('transaction_id')
        self.msg = args[0] if len(args) > 0 else ""

    def __str__(self):
        # TransactionIDAlreadyExists: tx(type='withdrawal', client='001', tx='001', amount='15.00')
        return "{}: {}".format(str(self.__class__).split(".")[1].strip('\'>'), self.msg)


class DepositError(PaymentError):
    pass


class DisputeError(PaymentError):
    pass


class ChargeBackError(PaymentError):
    pass


class ResolveError(PaymentError):
    pass


class WithdrawalError(PaymentError):
    pass


class ClientNotFound(PaymentError):
    pass


class ClientAccountLocked(PaymentError):
    pass


class TransactionIDAlreadyExists(PaymentError):
    pass


class TransactionNotFound(PaymentError):
    pass


class PaymentManager:
    MAX_UINT16 = 65535
    MAX_UINT32 = 4294967295
    COLS = {'client':
                {'fields': ["client", "held", "available", "total", "locked"],
                 'encoding': 'UTF-16'},
            'tx':
                {'fields': ["type", "client", "tx", "amount"],
                 'encoding': 'UTF-32'}
            }

    def __init__(self, client_csv=None, transaction_csv=None):
        """
        Creates `client_accounts.csv` and `transactions.csv` for tracking transactions
        while processing. Note that transactions and client records are persistent after program runs.
        To start clean please provide new paths or delete created files each time before running.
        """
        if not client_csv:
            self.client_csv = pathlib.Path.cwd() / "client_accounts.csv"
            if not pathlib.Path(self.client_csv).exists():
                pathlib.Path(self.client_csv).touch()
        else:
            self.client_csv = client_csv

        if not transaction_csv:
            self.transaction_csv = pathlib.Path.cwd() / "transactions.csv"
            if not pathlib.Path(self.transaction_csv).exists():
                pathlib.Path(self.transaction_csv).touch()
        else:
            self.transaction_csv = transaction_csv

        if not pathlib.Path(self.transaction_csv).exists():
            raise FileNotFoundError(self.transaction_csv)

        if not pathlib.Path(self.client_csv).exists():
            raise FileNotFoundError(self.client_csv)

        self.clients = defaultdict(list)
        self.transactions = defaultdict(list)

    def new_client(self, *cid, **kwargs) -> object:
        # ignore if client exists
        r = self.get_record('client', True, *cid)
        nc = []
        for k in cid:
            if not k.strip():
                k = self.generate_id('client')
                nc.append(k)
            if k not in r:
                # create a new entry
                if kwargs:
                    self.clients[k].append(kwargs)
                else:
                    self.clients[k].append(dict(client=k, held="0.00", available="0.00", total="0.00", locked="False"))
        return self

    def get_record(self, index, unique, *keys) -> defaultdict:
        """
        index: client or tx
        keys: list of indexes
        returns: a list of successfully updated records
        unique: if unique return on the first found record
        """
        try:
            # get csv  header based on supported record type
            fields = self.COLS[index]['fields']
        except KeyError:
            raise KeyError("unsupported csv fields.")

        # get csv path and encoding to write to
        rec_path = self.client_csv if index == 'client' else self.transaction_csv
        encoding = "UTF-16" if index == 'client' else "UTF-32"

        records = defaultdict(list)

        # read 20MB  chunks
        with open(rec_path, 'r', encoding=encoding, buffering=20000000) as f:
            reader = csv.DictReader(f, fieldnames=fields)
            for line in reader:
                for k in keys:
                    if k.strip() in line[index].strip():
                        records[k.strip()].append(
                            {k.strip(): str(v).strip().replace('None', '') for k, v in line.items()})
                        if unique:
                            break

        return records

    def save_client_accounts(self) -> list:
        """
        Update existing client records or append new records to the end of the file
        returns: a list of successfully updated records
        """

        fields = self.COLS['client']['fields']
        encoding = self.COLS['client']['encoding']
        index = 'client'

        # tmp file for saving after update
        temp_path = NamedTemporaryFile(mode='w', delete=False)
        upd = []  # tracks a list of new records

        # read 20MB  chunks
        with open(self.client_csv, 'r', encoding=encoding, buffering=20000000) as csvfile, \
                open(temp_path.name, 'w', encoding=encoding) as csvtempfile:
            reader = csv.DictReader(csvfile, fieldnames=fields)
            writer = csv.DictWriter(csvtempfile, fieldnames=fields)

            for i, rec in self.clients.items():
                merge = {}
                [merge.update(c) for c in rec]
                # update line in client_accounts if record exists
                updated = False
                for row in reader:
                    # update existing record if exists
                    if row[index].strip() == merge[index].strip():
                        updated = True
                        row.update(**merge)
                        upd.append(row)
                        writer.writerow(row)
                        break
                    writer.writerow(row)
                # now write row to file with possible update
                if not updated:
                    writer.writerow(merge)
            # copy the rest to temp file
            [writer.writerow(row) for row in reader]
        # save temp file to client_accounts
        shutil.move(csvtempfile.name, self.client_csv)

        return upd

    def save_transactions(self) -> list:
        """
        Write to a tempfile and save to final copy in the end to avoid
        data corruption incase of interrupt.
        
        Writes transactions to file and ignores duplicates.
        Appends new records to the end of the file
        Note: Transactions are immutable
        """

        fields = self.COLS['tx']['fields']
        encoding = self.COLS['tx']['encoding']

        # tmp file for saving after update
        temp_path = NamedTemporaryFile(mode='w', delete=False)
        new_data = copy.deepcopy(self.transactions)
        upd = []  # tracks a list of new records

        # read 20MB  chunks
        with open(self.transaction_csv, 'r', encoding=encoding, buffering=20000000) as csvfile, \
                open(temp_path.name, 'w', encoding=encoding) as csvtempfile:
            reader = csv.DictReader(csvfile, fieldnames=fields)
            writer = csv.DictWriter(csvtempfile, fieldnames=fields)
            for row in reader:
                # make a copy of each row read
                writer.writerow(row)
                for i, t in self.transactions.items():
                    for rec in t:
                        if equal_csv_row_dict(row, rec):
                            # skip duplicate since row already written in line 256
                            upd.append(rec)
                            # remove updated record from new data
                            for m in upd:
                                for n in new_data[i]:
                                    if equal_csv_row_dict(m, n):
                                        new_data[i].remove(m)
                                        break
                            continue
            # append new records
            if new_data:
                for _, l in new_data.items():
                    for r in l:
                        upd.append(r)
                        writer.writerow(r)

        shutil.move(csvtempfile.name, self.transaction_csv)
        return upd

    def print_clients(self, with_header=False, encoding='UTF-16'):
        writer = csv.DictWriter(sys.stdout, fieldnames=self.COLS['client']['fields'])
        if with_header:
            writer.writeheader()

        # read 20MB  chunks
        with open(self.client_csv, 'r', encoding=encoding, buffering=20000000) as f:
            reader = csv.DictReader(f, fieldnames=self.COLS['client']['fields'])
            for row in reader:
                writer.writerow(row)

    def deposit(self, *data_dict):
        """
        Entry criteria
        -------------
        - Amount is +ve
        """
        # deposit transactions are unique per client
        # get all completed transactions with same tx to avoid duplicate execution
        keys = [k['tx'] for k in data_dict]
        self.transactions = self.get_record('tx', False, *keys)

        # get clients referenced in transactions
        cids = []
        for i in data_dict:
            cids.append(i['client'])

        # get clients referenced  in  transactions
        self.clients = self.get_record('client', True, *cids)

        new_c = []
        # create a client record if one does not exist
        for rec in data_dict:
            if rec['client'] not in self.clients:
                # update new client records
                new_c.append(rec['client'])

        # save new client records to disk
        if new_c:
            self.new_client(*new_c)

        # client must exist at this point
        if not self.clients:
            raise ClientNotFound("Could not save client record to disk: " + pprint.pformat(new_c))

        for i in data_dict:
            cx = self.clients[i['client']][0]

            # perform required operation
            cx["total"] = add(cx["total"], i['amount'])
            cx["available"] = add(cx["available"], i['amount'])

            # write successful tx to disk
            self.transactions[i['tx'].strip()].append(i)

    def withdrawal(self, *data_dict):
        """
        Entry Criteria
        -------------
        - Client exist
        - Amount is +ve
        - Available amount > Withdrawal amount
        """
        for i in data_dict:
            # get referenced client
            self.clients = self.get_record('client', True, i['client'])
            cx = self.clients[i['client']][0]
            if cx['available'] and Decimal(cx['available']) < Decimal(i['amount']):
                raise WithdrawalError("Insufficient funds: " + pprint.pformat(i))

            # perform withdrawal action
            cx["total"] = subtract(cx["total"], i['amount'])
            cx["available"] = subtract(cx["available"], i['amount'])

            # save successful transaction
            self.transactions[i['tx'].strip()].append(i)

    def dispute(self, *data_dict):
        """
        Entry Criteria
        --------
        @see dispute_criteria_ok
        """
        # fetch processed transactions
        current = self.get_record('tx', False, *[k['tx'] for k in data_dict])

        # get clients referenced in transactions
        ids = []
        for tx, v in current.items():
            ids.extend(i['client'] for i in v)

        # get clients referenced  in  transactions
        self.clients = self.get_record('client', True, *ids)

        for i in data_dict:
            self.dispute_criteria_ok(i, self.clients, current)

            disp_amount = self.get_disputed_amount(current[i['tx']])

            # ignore locked accounts
            cx = self.clients[i['client']][0]

            if cx['available'] and Decimal(cx['available']) < Decimal(disp_amount):
                raise DisputeError("Insufficient funds :" + pprint.pformat(i))
            # hold disputed amount
            cx["held"] = add(cx['held'], disp_amount)
            cx["available"] = subtract(cx["available"], disp_amount)

            # save successful transaction
            self.transactions[i['tx'].strip()].append(i)

    def resolve(self, *data_dict):
        """
        Entry Criteria
        --------------
        @see dispute_pending
        @see dispute_critera_ok

        """
        # resolve transactions must already exist
        cur = self.get_record('tx', False, *[k['tx'] for k in data_dict])

        # get clients referenced in transactions
        ids = []
        for tx, v in cur.items():
            ids.extend(i['client'] for i in v)

        # get clients referenced  in  transactions
        self.clients = self.get_record('client', True, *ids)

        for i in data_dict:
            self.dispute_pending(i, self.clients, cur)

            disp_amount = self.get_disputed_amount(cur[i['tx']])

            # at this point we have a valid resolve transaction
            cx = self.clients[i['client']][0]

            if cx['held'] and Decimal(cx['held']) > Decimal(disp_amount):
                ResolveError("Insufficient funds :" + pprint.pformat(i))

            # move held amount to available funds
            cx["held"] = subtract(cx["held"], disp_amount)
            cx["available"] = add(cx["available"], disp_amount)

            # save successful transaction
            self.transactions[i['tx'].strip()].append(i)

    def chargeback(self, *data_dict):
        """
        Entry Criteria
        -------
        - Dispute completed
        - Resolution completed
        - Available funds > chargeback amount
        """
        # resolve transactions must already exist
        curr = self.get_record('tx', False, *[k['tx'] for k in data_dict])

        for i, c in curr.items():
            if not [d for d in c if d['type'] == 'dispute']:
                raise DisputeError("No dispute found: " + pprint.pformat(curr[i]))
            if not [d for d in c if d['type'] == 'resolve']:
                raise ResolveError("No Resolve found: " + pprint.pformat(curr[i]))

        # get clients referenced in transactions
        ids = []
        for tx, v in curr.items():
            ids.extend(i['client'] for i in v)

        # get clients referenced  in  transactions
        self.clients = self.get_record('client', True, *ids)

        for i in data_dict:
            cx = self.clients[i['client']][0]

            disp_amt = self.get_disputed_amount(curr[i['tx']])

            if cx["available"] and Decimal(cx["available"]) < disp_amt:
                raise ChargeBackError("Insufficient Funds: " + pprint.pformat(i))

            cx["available"] = subtract(cx["available"], disp_amt)
            cx["total"] = subtract(cx["total"], disp_amt)
            cx["locked"] = "True"

            # save successful transaction
            self.transactions[i['tx'].strip()].append(i)

    @staticmethod
    def get_disputed_amount(ts) -> Decimal:
        amount = []
        for m in ts:
            if m['amount'].strip() and Decimal(m['amount']):
                amount.append(Decimal(m['amount']))
        if not amount:
            raise DisputeError("Missing disputed amount: " + pprint.pformat(ts))
        if len(amount) > 1:
            raise DisputeError("Multiple disputed amounts: " + pprint.pformat(ts))
        return amount[0]

    def dispute_pending(self, tx, clients=None, transactions=None):
        """
        Criteria
        --------
        - dispute_criteria_ok
        - held amount and existing dispute
        """
        if not clients:
            clients = self.get_record('client', tx['client'])
        if not clients:
            raise ClientNotFound(pprint.pformat(tx))

        if not transactions:
            transactions = self.get_record('tx', tx['tx'])
        if not transactions:
            raise TransactionNotFound("Transaction does not exist: " + pprint.pformat(tx))

        cs = clients[tx['client']][0]
        ts = transactions[tx['tx']]

        if cs['held'] and Decimal(cs['held']) > 0 and \
                [d for d in transactions[tx['tx']] if d['type'] == 'dispute']:
            return True
        raise DisputeError("No Valid pending Dispute for this tx: " + pprint.pformat(tx))

    def resolve_pending(self, tx, clients=None, transactions=None):
        """
        Criteria
        --------
        - held amount and cleared dispute
        """
        if not clients:
            clients = self.get_record('client', tx['client'])
        if not clients:
            raise ClientNotFound(pprint.pformat(tx))

        if not transactions:
            transactions = self.get_record('tx', tx['tx'])
        if not transactions:
            raise TransactionNotFound("Transaction does not exist: " + pprint.pformat(tx))

        cs = clients[tx['client']][0]
        ts = transactions[tx['tx']]

        if cs['held'] and Decimal(cs['held']) > 0 and \
                [d for d in transactions[tx['tx']] if d['type'] == 'dispute']:
            return True
        raise DisputeError("No Valid pending Dispute for this tx: " + pprint.pformat(tx))

    def dispute_criteria_ok(self, tx, clients=None, transactions=None):
        """
        Criteria (Called by Dispute func)
        --------
        - Client Must exist
        - Atleast one Deposit made (disputed amount)
        """
        if not clients:
            clients = self.get_record('client', tx['client'])
        if not clients:
            raise ClientNotFound(pprint.pformat(tx))

        if not transactions:
            transactions = self.get_record('tx', tx['tx'])
        if not transactions:
            raise TransactionNotFound("Transaction does not exist: " + pprint.pformat(tx))

        cs = clients[tx['client']][0]
        ts = transactions[tx['tx']]

        deposit_exist = []
        deposit_exist.extend([d for d in ts if d['type'] == 'deposit'])

        disputed_amount = []
        disputed_amount.extend(
            [d['amount'] for d in ts if d['amount'] and Decimal(d['amount']) <= Decimal(cs['available'])])
        if not disputed_amount:
            raise DisputeError("Missing disputed amount: " + pprint.pformat(tx))

        if len(disputed_amount) > 1:
            raise DisputeError("Multiple disputed amount: " + pprint.pformat(tx))

        if not deposit_exist:
            raise DisputeError("Invalid tx: No deposit exist for this transaction: " + pprint.pformat(tx))

    @staticmethod
    def valid_id_or_fail(tx):
        # get id from record
        c = tx.get('client')
        t = tx.get('tx')

        if c and bin(int(str(c))).count('1') > 16:
            raise ValueError("Invalid `{}` id in {}".format(c, tx))

        if t and bin(int(str(t))).count('1') > 32:
            raise ValueError("Invalid `{}` id in {}".format(t, tx))
        if not (c or t):
            raise ValueError("Missing transaction id(s) in `{}`".format(tx))

    def generate_id(self, typ):
        m = self.MAX_UINT16 if typ == 'tx' else self.MAX_UINT32
        for i in range(10):  # ten tries and quit
            i = randrange(1, m)
            if not self.get_record('client', True, i):
                return str(i)
        raise Exception("Cannot generate new `typ` id")

    def validate(self, tx):
        """
        General criteria
        ------------------
        - Tx id must be a valid u32
        - Client id must be a valid u16
        - Only 1 valid tx operation per tx id
        - Amount cannot be -ve
        - Client MUST EXIST for all operations except deposit
        - If Client exists, account  must not be locked
        """
        # verify tx and client id
        # create new client if performing a `deposit` and client id is empty
        try:
            if not tx['client']:
                if tx['type'] == 'deposit':
                    tx['client'] = self.generate_id('client')
        except KeyError:
            pass
        else:
            self.valid_id_or_fail(tx)

        # skip locked accounts
        client = self.get_record('client', True, tx['client'])
        if client:
            if ast.literal_eval(client[tx['client']][0]['locked'].strip()):
                raise ClientAccountLocked(client[tx['client']][0])
        existing_tx = self.get_record('tx', False, tx['tx'])

        # skip -negative
        if tx['amount'].strip():
            if Decimal(tx['amount']) <= 0:
                raise PaymentError("Amounts cannot be negative: " + pprint.pformat(tx))

        # only one operation allowed  at a time per tx id
        for i in existing_tx[tx['tx']]:
            if tx['type'] == i['type']:
                raise TransactionIDAlreadyExists(
                    "Same operation exists for the same tx id: " + pprint.pformat(tx))
        if not existing_tx:
            raise DisputeError("Transaction not found: " + pprint.pformat(tx))

        # new account is created for a deposit  operation if one does not exist
        if tx['type'] != 'deposit':
            if not client:
                raise ClientNotFound(tx)
        if tx['type'] not in ['deposit', 'withdrawal']:
            dispute_amount = self.get_disputed_amount(existing_tx[tx['tx']])

        if tx['type'] == "deposit":
            """
            Deposit Criteria
            ----------------
            - Tx ID Must be unique per deposit
            - Client does NOT have to exist (A new is created is one does not exist)
            - Amount must be valid Decimal(s) > 0
            """
            pass

        elif tx['type'] == "withdrawal":
            """
            Withdrawal Criteria
            --------------------
            - Tx ID Must be unique per deposit
            - Client MUST exist
            - Amount must be valid Decimal(s) > 0
            """
            # skip if client does not exist
            if not client:
                raise ClientNotFound(pprint.pformat(tx))

            # bail if insufficient funds
            # when run as a  script processing is via line by line streaming and processing
            if __name__ == '__main__':
                if Decimal(client[tx['client']][0]["available"]) < Decimal(tx['amount']):
                    raise WithdrawalError("Insufficient Funds: " + pprint.pformat(tx))

        elif tx['type'] == "dispute":
            """
            Dispute Criteria
            ------------------
            - Tx ID Must already exist and not include include:
                - Completed resolve
            - Client MUST exist
            - Amount must be valid Decimal(s) > 0
            - Must contain exactly one disputed amount
            - Available amount MUST be greater than disputed amount
            """
            # when run as a  script processing is via line by line streaming and processing
            if __name__ == '__main__':
                if dispute_amount > Decimal(client[tx['client']][0]['available']):
                    raise DisputeError("Insufficient amount: " + pprint.pformat(tx))
                else:
                    return True

        elif tx['type'] == "resolve":
            """
            Resolve Criteria
            --------------------
            - Tx ID Must already exist and include:
                - Completed dispute
                - Valid Disputed amount
            - Client MUST exist
            - Amount must be valid Decimal(s) > 0
            """
            # when run as a  script processing is via line by line streaming and processing
            if __name__ == '__main__':
                if dispute_amount > Decimal(client[tx['client']][0]['available']):
                    raise DisputeError("Insufficient amount: " + pprint.pformat(tx))

            # check for a successful dispute
            disputed = []
            disputed.extend([m for m in existing_tx[tx['tx']] if m['type'] == 'disputed'])
            if not disputed:
                return ResolveError("Undisputed Transaction: " + pprint.pformat(tx))
            for i in disputed:
                if Decimal(i['held'] > dispute_amount):
                    return True
            raise ResolveError("Transaction has insufficient held amount: " + pprint.pformat(tx))

        elif tx['type'] == "chargeback":
            """
            Chargeback Criteria
            ------------------
            - Tx ID Must already exist and include:
                - Completed dispute
                - Completed resolve
                - Disputed amount
            - Client MUST exist
            - Amount must be valid Decimal(s) > 0
            """
            cx = client[tx['client']][0]
            # when run as a  script processing is via line by line streaming and processing
            if __name__ == '__main__':
                if dispute_amount > Decimal(cx['available']):
                    raise DisputeError("Insufficient amount: " + pprint.pformat(tx))

            # check for a successful dispute
            disputed = []
            disputed.extend([m for m in existing_tx[tx['tx']] if m['type'] == 'dispute'])
            # check for resolved
            resolved = []
            resolved.extend([m for m in existing_tx[tx['tx']] if m['type'] == 'resolve'])

            if not disputed:
                return DisputeError("Undisputed Transaction: " + pprint.pformat(tx))
            if not resolved:
                return ResolveError("Transaction not yet resolved: " + pprint.pformat(tx))
            if Decimal(cx['available']) >= dispute_amount:
                return True
            raise ChargeBackError("Insufficient Funds: " + pprint.pformat(tx))
        else:
            raise PaymentError("Invalid  Tx type: " + pprint.pformat(tx))
        return True


def process(*data_dict, **kwargs):
    """
    Calls transaction action based on type field from csv 
    """
    p = PaymentManager(**kwargs)

    for d in data_dict:
        try:
            if not p.validate(d):
                # ignore invalid transactions
                # Todo: log these
                continue
            getattr(p, d['type'])(d)
            # save successful transaction
            if p.clients:
                p.save_client_accounts()
            if p.transactions:
                p.save_transactions()

        except PaymentError as err:
            # ignore if streaming a csv file as main
            if __name__ == '__main__':
                if os.getenv('DEBUG'):
                    print(err)
            else:
                return p, err
    return p


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(0)

    if len(sys.argv) == 2:
        if not pathlib.Path(sys.argv[1]).exists():
            tx_path = pathlib.Path(sys.argv[0]).parent.parent / "assets" / sys.argv[1]
        else:
            tx_path = sys.argv[1]
    else:
        sys.exit(0)
    if not pathlib.Path(tx_path).exists():
        raise FileNotFoundError(tx_path)

    # read 20MB  chunks
    data_dict = []

    with open(tx_path, 'r', encoding='UTF-32', buffering=20000000) as csvfile:
        print(','.join(PaymentManager.COLS['client']['fields']))
        reader = csv.DictReader(csvfile, fieldnames=PaymentManager.COLS['tx']['fields'])

        # skip header
        next(reader)
        mgr = None
        for row in reader:
            #  process row by row
            try:
                mgr = process({k.strip(): str(v).strip().replace('None', '') for k, v in row.items()})
            except PaymentError as err:
                # print(err)
                if os.getenv('DEBUG'):
                    print(err)

            # write to stdout
            # if mgr.clients:
            #    mgr.print_clients()
            # print client_accounts to stdout
        if mgr:
            mgr.print_clients()
