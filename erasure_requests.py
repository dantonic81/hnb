from dagster import graph, op
from erasure_requests_etl import main


@op
def erasure_requests_op():
    main()


@graph
def erasure_requests_graph():
    erasure_requests_op()
