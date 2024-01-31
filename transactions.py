from dagster import graph, op
from transactions_etl import main


@op
def process_transactions_op():
    main()


@graph
def transactions_graph():
    process_transactions_op()
