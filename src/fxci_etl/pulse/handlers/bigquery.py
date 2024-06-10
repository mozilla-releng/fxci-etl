from dataclasses import dataclass

from fxci_etl.config import Config
from fxci_etl.loaders.bigquery import BigQueryLoader, Record
from fxci_etl.pulse.handlers.base import PulseHandler, register


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

    def __str__(self):
        return f"{self.taskId} run {self.runId}"


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

    def __str__(self):
        return self.taskId


@register()
class BigQueryHandler(PulseHandler):
    name = "bigquery"

    def __init__(self, config: Config):
        super().__init__(config)
        self.loader = BigQueryLoader(self.config)

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
        self.loader.insert(Run.from_dict(run_record))

        if run_record["runId"] == 0:
            # Only insert the task record for run 0 to avoid duplicate records.
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

            self.loader.insert(Task.from_dict(task_record))
