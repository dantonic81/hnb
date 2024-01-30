from dagster import DefaultScheduleStatus, FilesystemIOManager, graph, op, repository, schedule
from customers import customers_graph
from transactions import transactions_graph
from products import products_graph
from erasure_requests import erasure_requests_graph


customers_job = customers_graph.to_job(name="customers_job")
products_job = products_graph.to_job(name="products_job")
transactions_job = transactions_graph.to_job(name="transactions_job")
erasure_requests_job = erasure_requests_graph.to_job(name="erasure_requests_job")


@schedule(cron_schedule="0 * * * *", job=customers_job, execution_timezone="Europe/Zagreb", default_status=DefaultScheduleStatus.STOPPED)
def customers_schedule(_context):
    return {}


@schedule(cron_schedule="0 * * * *", job=transactions_job, execution_timezone="Europe/Zagreb", default_status=DefaultScheduleStatus.STOPPED)
def transactions_schedule(_context):
    return {}


@schedule(cron_schedule="0 0 * * *", job=products_job, execution_timezone="Europe/Zagreb", default_status=DefaultScheduleStatus.STOPPED)
def products_schedule(_context):
    return {}


@schedule(cron_schedule="0 1 * * *", job=erasure_requests_job, execution_timezone="Europe/Zagreb", default_status=DefaultScheduleStatus.STOPPED)
def erasure_requests_schedule(_context):
    return {}


@repository
def deploy_docker_repository():
    return [customers_schedule, transactions_schedule, products_schedule, erasure_requests_schedule, customers_job, products_job, transactions_job, erasure_requests_job]
