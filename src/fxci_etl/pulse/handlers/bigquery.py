from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from pprint import pprint
from typing import Any

import dacite
from google.cloud.bigquery import Client

from fxci_etl.config import Config
from fxci_etl.pulse.handlers.base import PulseHandler, register


@dataclass
class Record(ABC):

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Record":
        return dacite.from_dict(data_class=cls, data=data)

    @classmethod
    @abstractmethod
    def table_name(cls) -> str: ...


@dataclass
class Run(Record):
    reasonCreated: str
    reasonResolved: str
    resolved: str
    runId: int
    scheduled: str
    started: str
    state: str
    taskId: str
    workerGroup: str
    workerId: str

    @classmethod
    def table_name(cls):
        return "task_runs"


@dataclass
class Task(Record):
    provisionerId: str
    schedulerId: str
    tags: dict[str, str]
    taskGroupId: str
    taskId: str
    taskQueueId: str
    workerType: str

    @classmethod
    def table_name(cls):
        return "tasks"


@register()
class BigQueryHandler(PulseHandler):
    name = "bigquery"

    def __init__(self, config: Config):
        super().__init__(config)

        bq = self.config.bigquery
        self.client = Client()
        self.tables = {
            name: self.client.get_table(f"{bq.project}.{bq.dataset}.{table}")
            for name, table in bq.tables.items()
        }

    def insert(self, row: Record):
        table = self.tables[row.table_name()]
        errors = self.client.insert_rows(table, [asdict(row)])
        pprint(errors, indent=2)

    def __call__(self, data, message):
        run_record = {"taskId": data["status"]["taskId"]}

        for key in ("runId", "workerGroup", "workerId"):
            run_record[key] = data[key]

        for key in (
            "reasonCreated",
            "reasonResolved",
            "resolved",
            "scheduled",
            "started",
            "state",
            "workerGroup",
            "workerId",
        ):
            run_record[key] = data["status"]["runs"][-1][key]
        self.insert(Run.from_dict(run_record))

        task_record = {"tags": data["task"]["tags"]}
        for key in (
            "provisionerId",
            "schedulerId",
            "taskGroupId",
            "taskId",
            "taskQueueId",
            "workerType",
        ):
            task_record[key] = data["status"][key]

        self.insert(Task.from_dict(task_record))
