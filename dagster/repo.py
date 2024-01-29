from dagster import DefaultScheduleStatus, FilesystemIOManager, graph, op, repository, schedule
from dagster_docker import docker_executor
from customers import customers_graph
from transactions import transactions_graph
from erasure_requests import erasure_requests_graph
from products import products_graph

# @op
# def hello():
#     return 1


# @op
# def goodbye(foo):
#     if foo != 1:
#         raise Exception("Bad io manager")
#     return foo * 2


# @graph
# def my_graph():
#     goodbye(hello())


# my_job = my_graph.to_job(name="my_job")

# my_step_isolated_job = my_graph.to_job(
#     name="my_step_isolated_job",
#     executor_def=docker_executor,
#     resource_defs={"io_manager": FilesystemIOManager(base_dir="/tmp/io_manager_storage")},
# )

customers_job = customers_graph.to_job(name="customers_job")
products_job = products_graph.to_job(name="products_job")
transactions_job = transactions_graph.to_job(name="transactions_job")
erasure_requests_job = erasure_requests_graph.to_job(name="erasure_requests_job")


@schedule(cron_schedule="*/2 * * * *", job=customers_job, execution_timezone="Europe/Zagreb", default_status=DefaultScheduleStatus.STOPPED)
def my_schedule(_context):
    return {}


@repository
def deploy_docker_repository():
    # return [my_job, my_step_isolated_job, my_schedule, my_customers_job]
    return [my_schedule, customers_job, products_job, transactions_job, erasure_requests_job]
