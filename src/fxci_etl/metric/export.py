import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pprint import pprint

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.monitoring_v3 import (
    Aggregation,
    ListTimeSeriesRequest,
    MetricServiceClient,
    TimeInterval,
)
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from fxci_etl.config import Config
from fxci_etl.loaders.bigquery import BigQueryLoader, Record

METRIC = "compute.googleapis.com/instance/uptime"
SAMPLE_HOURS = 6


@dataclass
class WorkerUptime(Record):
    instance_id: str
    project: str
    zone: str
    uptime: float
    start_time: float
    end_time: float

    @classmethod
    def table_name(cls):
        return "worker_uptime"

    def __str__(self):
        return f"worker {self.instance_id}"


def get_timeseries(project: str, interval: TimeInterval):
    client = MetricServiceClient()

    metric_filter = f'metric.type="{METRIC}"'

    aggregation = Aggregation(
        alignment_period=Duration(
            seconds=int(interval.end_time.timestamp())
            - int(interval.start_time.timestamp())
        ),
        per_series_aligner=Aggregation.Aligner.ALIGN_SUM,
        cross_series_reducer=Aggregation.Reducer.REDUCE_SUM,
        group_by_fields=[
            "metric.labels.instance_name",
            "resource.labels.instance_id",
            "resource.labels.zone",
        ],
    )

    results = client.list_time_series(
        request={
            "name": f"projects/{project}",
            "filter": metric_filter,
            "interval": interval,
            "view": ListTimeSeriesRequest.TimeSeriesView.FULL,
            "aggregation": aggregation,
        }
    )

    return results


def get_time_interval(dry_run: bool = False) -> TimeInterval:
    client = storage.Client()
    bucket = client.bucket("fxci-etl")
    blob = bucket.blob("last_uptime_export_interval.json")

    # Set end time to ten minutes in the past to ensure Google Cloud Monitoring
    # has finished computing all of its metrics.
    end_time = datetime.now() - timedelta(minutes=10)
    try:
        start_time = json.loads(blob.download_as_string())["end_time"]
    except NotFound:
        start_time = int((end_time - timedelta(hours=SAMPLE_HOURS)).timestamp())

    end_time = int(end_time.timestamp())

    if start_time > end_time:
        raise Exception("Abort: metric export ran too recently!")

    if not dry_run:
        blob.upload_from_string(json.dumps({"end_time": end_time}))

    return TimeInterval(
        end_time=Timestamp(seconds=end_time), start_time=Timestamp(seconds=start_time)
    )


def export_metrics(config: Config, dry_run: bool = False) -> int:
    interval = get_time_interval(dry_run)

    records = []
    for project in ("fxci-production-level1-workers", "fxci-production-level3-workers"):
        for ts in get_timeseries(project, interval):
            if dry_run:
                pprint(ts)
                continue

            data = {
                "project": ts.resource.labels["project_id"],
                "zone": ts.resource.labels["zone"],
                "instance_id": ts.resource.labels["instance_id"],
                "uptime": round(ts.points[0].value.double_value, 2),
                "start_time": ts.points[0].interval.start_time.timestamp(),
                "end_time": ts.points[0].interval.end_time.timestamp(),
            }
            records.append(WorkerUptime.from_dict(data))

    if records:
        loader = BigQueryLoader(config)
        loader.insert(records)
    return 0
