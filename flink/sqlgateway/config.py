from dataclasses import dataclass


@dataclass
class SqlGatewayConfig:
    host: str
    port: int
    session_name: str

    def gateway_url(self) -> str:
        return f"http://{self.host}:{self.port}"
