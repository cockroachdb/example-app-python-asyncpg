# example-app-python-asyncpg
This repo has a simple CRUD Python application that uses the [asyncpg](https://magicstack.github.io/asyncpg/current/index.html) driver.

## Before you begin

To run this example you must have:

- Python 3
- A CockroachDB cluster v23.1 or greater

## Install the dependencies

In a terminal run the following command:

~~~ shell
pip install -r requirements.txt
~~~

## Set the CockroachDB connection URL

In a terminal set a `DATABASE_URL` environment variable to the connection URL for your CockroachDB cluster.

For example on Mac and Linux:

~~~ shell
export DATABASE_URL='<connection URL>'
~~~

## Run the example

To run the example, in a terminal run the following command:

~~~ shell
python3 example.py
~~~
