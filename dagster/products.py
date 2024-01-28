from dagster import graph, op

@op
def products_op():
    pass


@graph
def products_graph():
    products_op()

