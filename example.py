#!/usr/bin/env python3
"""
Use asyncpg with CockroachDB.
"""

import logging
import os
import random
import time
import uuid
from argparse import ArgumentParser, RawTextHelpFormatter
import asyncio
import asyncpg
from asyncpg.exceptions import SerializationError

async def create_accounts(conn):
    id1 = uuid.uuid4()
    id2 = uuid.uuid4()
    
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS accounts (id UUID PRIMARY KEY, balance INT)"
    )
    results = await conn.execute(
        "UPSERT INTO accounts (id, balance) VALUES ($1, 1000), ($2, 250)", id1, id2)
    logging.debug("create_accounts(): status message: %s",
                  results)
    return [id1, id2]


async def delete_accounts(conn):
    result = await conn.execute("DELETE FROM accounts")
    logging.debug("delete_accounts(): status message: %s",
                  result)


async def print_balances(conn):
    print(f"Balances at {time.asctime()}:")
    for row in await conn.fetch("SELECT id, balance FROM accounts"):
        print("account id: {0}  balance: ${1:2d}".format(row['id'], row['balance']))


async def transfer_funds(conn, frm, to, amount):
    # Check the current balance.
    from_balance = await conn.fetchrow("SELECT balance FROM accounts WHERE id = $1", frm)
    if from_balance['balance'] < amount:
        raise RuntimeError(
            f"insufficient funds in {frm}: have {from_balance}, need {amount}"
        )

    # Perform the transfer.
    await conn.execute(
        "UPDATE accounts SET balance = balance - $1 WHERE id = $2", 
            amount, frm
    )
    results = await conn.execute(
        "UPDATE accounts SET balance = balance + $1 WHERE id = $2", 
            amount, to
    )

    logging.debug("transfer_funds(): status message: %s", results)


async def run_transaction(conn, op, max_retries=3):
    """
    Execute the operation *op(conn)* retrying serialization failure.

    If the database returns an error asking to retry the transaction, retry it
    *max_retries* times before giving up (and propagate it).
    """
    # leaving this block the transaction will commit or rollback
    # (if leaving with an exception)
    async with conn.transaction():
        for retry in range(1, max_retries + 1):
            try:
                await op(conn)

                # If we reach this point, we were able to commit, so we break
                # from the retry loop.
                return

            except SerializationError as e:
                # This is a retry error, so we roll back the current
                # transaction and sleep for a bit before retrying. The
                # sleep time increases for each failed transaction.
                logging.debug("got error: %s", e)
                conn.rollback()
                logging.debug("EXECUTE SERIALIZATION_FAILURE BRANCH")
                sleep_seconds = (2**retry) * 0.1 * (random.random() + 0.5)
                logging.debug("Sleeping %s seconds", sleep_seconds)
                time.sleep(sleep_seconds)

            except Exception as e:
                logging.debug("got error: %s", e)
                logging.debug("EXECUTE NON-SERIALIZATION_FAILURE BRANCH")
                raise e

        raise ValueError(
            f"transaction did not succeed after {max_retries} retries")


async def main():
    opt = parse_cmdline()
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO)
    try:
        # Attempt to connect to cluster with connection string provided to
        # script. By default, this script uses the value saved to the
        # DATABASE_URL environment variable.
        # For information on supported connection string formats, see
        # https://www.cockroachlabs.com/docs/stable/connect-to-the-database.html.
        db_url = opt.dsn
        conn = await asyncpg.connect(db_url, 
                               server_settings={"application_name": "docs_simplecrud_asyncpg"})
        # Set the multiple_active_portals_enabled session variable to true
        await conn.execute("SET multiple_active_portals_enabled = true")
        ids = await create_accounts(conn)
        await print_balances(conn)
            
        amount = 100
        toId = ids.pop()
        fromId = ids.pop()

        try:
            await run_transaction(conn, lambda conn: transfer_funds(conn, fromId, toId, amount))
        except ValueError as ve:
            # Below, we print the error and continue on so this example is easy to
            # run (and run, and run...).  In real code you should handle this error
            # and any others thrown by the database interaction.
            logging.debug("run_transaction(conn, op) failed: %s", ve)
            pass
        except Exception as e:
            logging.debug("got error: %s", e)
            raise e

        await print_balances(conn)

        await delete_accounts(conn)
    except Exception as e:
        logging.fatal("database connection failed")
        logging.fatal(e)
        return
    
    finally:
        await conn.close()


def parse_cmdline():
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawTextHelpFormatter)

    parser.add_argument("-v", "--verbose",
                        action="store_true", help="print debug info")

    parser.add_argument(
        "dsn",
        default=os.environ.get("DATABASE_URL"),
        nargs="?",
        help="""\
database connection string\
 (default: value of the DATABASE_URL environment variable)
            """,
    )

    opt = parser.parse_args()
    if opt.dsn is None:
        parser.error("database connection string not set")
    return opt


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())