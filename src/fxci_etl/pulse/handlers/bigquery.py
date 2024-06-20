from dataclasses import dataclass
from typing import Any

from fxci_etl.config import Config
from fxci_etl.loaders.bigquery import BigQueryLoader, Record
from fxci_etl.pulse.handlers.base import PulseHandler, register


@dataclass
class Run(Record):
    reason_created: str
    reason_resolved: str
    resolved: str
    run_id: int
    scheduled: str
    started: str
    state: str
    task_id: str
    worker_group: str
    worker_id: str

    @classmethod
    def table_name(cls):
        return "task_runs"

    def __str__(self):
        return f"{self.task_id} run {self.run_id}"


@dataclass
class Tag:
    key: str
    value: str


@dataclass
class Task(Record):
    scheduler_id: str
    task_group_id: str
    task_id: str
    task_queue_id: str
    tags: list[Tag]

    @classmethod
    def table_name(cls):
        return "tasks"

    def __str__(self):
        return self.task_id


@register()
class BigQueryHandler(PulseHandler):
    name = "bigquery"

    def __init__(self, config: Config, **kwargs: Any):
        super().__init__(config, **kwargs)
        self.loader = BigQueryLoader(self.config)

    def process_events(self, events):
        records = []
        for event in events:
            data = event.data
            status = data["status"]
            run = data["status"]["runs"][-1]
            records.append(
                Run.from_dict(
                    {
                        "task_id": status["taskId"],
                        "reason_created": run["reasonCreated"],
                        "reason_resolved": run["reasonResolved"],
                        "resolved": run["resolved"],
                        "run_id": data["runId"],
                        "scheduled": run["scheduled"],
                        "started": run["started"],
                        "state": run["state"],
                        "worker_group": run["workerGroup"],
                        "worker_id": run["workerId"],
                    }
                )
            )

            if data["runId"] == 0:
                # Only insert the task record for run 0 to avoid duplicate records.
                records.append(
                    Task.from_dict(
                        {
                            "scheduler_id": status["schedulerId"],
                            "tags": [
                                {"key": k, "value": v}
                                for k, v in data["task"]["tags"].items()
                            ],
                            "task_group_id": status["taskGroupId"],
                            "task_id": status["taskId"],
                            "task_queue_id": status["taskQueueId"],
                        }
                    )
                )

        if records:
            self.loader.insert(records)
