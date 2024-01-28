from dagster import graph, op
from callables.customer_etl import main


@op
def process_customers_op(context):
    context.log.info("From customer op!")
    result = main()
    context.log.info(result)


@graph
def customers_graph():
    process_customers_op()
