from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.hooks.base import BaseHook
from pendulum import datetime

ACCOUNT_ID = "account-id"
ATOM_ID = "atom-id"
PROCESS_ID = "process-id"
BASE_URL = f"https://api.boomi.com/api/rest/v1/{ACCOUNT_ID}"


def boomi_auth():
    conn = BaseHook.get_connection("boomi")
    return (conn.login, conn.password)


@dag(schedule="* * * * *", start_date=datetime(2026, 11, 1), catchup=False, max_active_runs=1)
def boomi_process():

    @task
    def trigger_process() -> str:
        import requests

        response = requests.post(
            f"{BASE_URL}/ExecutionRequest",
            auth=boomi_auth(),
            headers={"Accept": "application/json"},
            json={
                "@type": "ExecutionRequest",
                "atomId": ATOM_ID,
                "processId": PROCESS_ID,
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["recordUrl"]

    @task.sensor(poke_interval=30, timeout=3600)
    def wait_for_completion(record_url: str) -> PokeReturnValue:
        import requests

        response = requests.get(
            record_url,
            auth=boomi_auth(),
            headers={"Accept": "application/json"},
            timeout=30,
        )
        if response.status_code == 202:  # record not available yet
            return PokeReturnValue(is_done=False)
        response.raise_for_status()

        record = response.json()
        if "result" in record:  # in case the record comes wrapped in a result list
            record = record["result"][0]
        status = record["status"]
        print(f"Boomi status: {status}")

        if status in ("COMPLETE", "COMPLETE_WARN"):
            return PokeReturnValue(is_done=True, xcom_value=record)
        if status in ("ERROR", "ABORTED", "DISCARDED"):
            raise RuntimeError(f"Boomi process ended with status {status}")
        return PokeReturnValue(is_done=False)  # INPROCESS / STARTED

    wait_for_completion(trigger_process())


boomi_process()