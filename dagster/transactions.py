from dagster import graph, op


@op
def transactions_op():
    pass


@graph
def transactions_graph():
    transactions_op()
