Payment Gateway Demo
================

## Assumptions, covered cases, encoding
- Transaction ids are unique per client and payment operations
- client_accounts.csv decodes UTF-32
- transactions.csv decodes UTF-16
- All payment operations are covered (see uploaded unittests for coverage)
- processing is done on streamed blocks of file and done line by line
- maybe slow to process large files but does not use a ton of memory per processing

## Project Structure
```
payment_gateway
   |--assets # sample test files
   |--main
       |--client_accounts.csv
       |--transactions.csv
       |--payment_gateway.csv
   |--test
       |-client_accounts.csv
       |-test.py
       |-transactions.csv
```

## Running program
```
$ cd payment_gateway
$ python3 python3 main/payment_gateway.py assets/tx1.csv 
```

## Running unittest

```
$ python3 -m unittest -v

test_deposit_for_existing_client (test.test.Test) ... ok
test_deposit_for_new_client (test.test.Test)
type,       client,     tx,      amount ... ok
test_deposit_locked_account (test.test.Test) ... ok
test_duplicate_chargeback (test.test.Test) ... ok
test_duplicate_deposit_for_existing_client (test.test.Test) ... ok
test_duplicate_despute (test.test.Test) ... ok
test_duplicate_resolve (test.test.Test) ... ok
test_insufficient_funds (test.test.Test) ... ok
test_invalid_client_id_u16_too_large (test.test.Test) ... ok
test_invalid_tx_encoding (test.test.Test) ... ok
test_invalid_tx_id_u32_non_integer (test.test.Test) ... ok
test_invalid_tx_id_u32_too_large (test.test.Test) ... ok
test_negative_deposit (test.test.Test) ... ok
test_negative_withdrawal (test.test.Test) ... ok
test_resolve_without_dispute (test.test.Test) ... ok
test_same_tx_deposit_for_multiple_clients (test.test.Test) ... ok
test_same_tx_withdrawal_for_multiple_clients (test.test.Test) ... ok
test_undisputed_chargeback (test.test.Test) ... ok
test_unresolved_chargeback (test.test.Test) ... ok
test_valid_chargeback (test.test.Test) ... ok
test_valid_client_encoding (test.test.Test) ... ok
test_valid_dispute (test.test.Test) ... ok
test_withdrawal_after_chargeback (test.test.Test) ... ok
test_withdrawal_for_existing_client (test.test.Test) ... ok
test_withdrawal_for_non_existent_client (test.test.Test) ... ok
test_withdrawal_locked_account (test.test.Test) ... ok

----------------------------------------------------------------------
Ran 26 tests in 0.145s

OK


