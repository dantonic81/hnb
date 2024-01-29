from dagster import graph, op
from customer_etl import main


@op
def process_customers_op():
    main()


@graph
def customers_graph():
    process_customers_op()
