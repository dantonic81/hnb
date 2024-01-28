from dagster import graph, op

@op
def customers_op():
    pass


@graph
def customers_graph():
    customers_op()
