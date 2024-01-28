from dagster import graph, op

@op
def erasure_requests_op():
    pass


@graph
def erasure_requests_graph():
    erasure_requests_op()
