from dagster import graph, op
from products_etl import main


@op
def process_products_op():
    main()


@graph
def products_graph():
    process_products_op()
