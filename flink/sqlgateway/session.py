from dataclasses import dataclass
from typing import Optional
import json

import requests
from flink.sqlgateway.config import SqlGatewayConfig


@dataclass
class SqlGatewaySession:
    config: SqlGatewayConfig
    session_handle: Optional[str] = None

    @staticmethod
    def create(config: SqlGatewayConfig) -> "SqlGatewaySession":
        session_request = {"sessionName": config.session_name}

        response = requests.post(
            url=f"{config.gateway_url()}/v1/sessions",
            data=json.dumps(session_request),
            headers={
                "Content-Type": "application/json",
            },
        )

        if response.status_code == 200:
            session_handle = response.json()["sessionHandle"]
            return SqlGatewaySession(config, session_handle)
        else:
            raise Exception("SQL gateway error: ", response.status_code)

    def session_endpoint_url(self) -> str:
        return f"{self.config.gateway_url()}/v1/sessions/{self.session_handle}"
