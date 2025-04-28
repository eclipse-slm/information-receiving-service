from abc import ABC
from datetime import datetime, timezone, timedelta
from enum import Enum

import requests
from oauthlib.oauth2 import BackendApplicationClient
from pydantic import BaseModel, Field
from requests import ConnectTimeout
from requests_oauthlib import OAuth2Session


class AuthType(str, Enum):
    OAUTH2 = "oauth2"
    APIKEY = "apikey"
    CUSTOM_OAUTH = "custom-oauth"


class AuthMethod(BaseModel, ABC):
    auth_type: AuthType = Field(alias="auth-type")


class Oauth2AuthMethod(AuthMethod, BaseModel):
    auth_type: AuthType = AuthType.OAUTH2
    client_id: str = Field(alias="client-id")
    client_secret: str = Field(alias="client-secret")
    token_url: str = Field(alias="token-url")
    _token: dict = {}

    @property
    def token(self) -> str:
        client = BackendApplicationClient(client_id=self.client_id)

        if not self._token or self._is_token_expired():
            session = OAuth2Session(client=client)
            token_dict = session.fetch_token(
                token_url=self.token_url,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            self._token = token_dict

        return self._token

    @property
    def auth_header(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token['access_token']}"
        }

    def _is_token_expired(self) -> bool:
        current_time_minus_one_minute = datetime.now(timezone.utc) + timedelta(minutes=1)
        expired_at_time = datetime.fromtimestamp(self._token['expires_at'],timezone.utc)

        if current_time_minus_one_minute >= expired_at_time:
            return True
        return False


class ApiKeyAuthMethod(AuthMethod, BaseModel):
    auth_type: AuthType = AuthType.APIKEY
    api_key: str = Field(alias="api-key")

    @property
    def auth_header(self) -> dict:
        return {
            "Authorization": f"{self.api_key}"
        }


class CustomOAuthMethod(AuthMethod, BaseModel):
    auth_type: AuthType = AuthType.CUSTOM_OAUTH
    client_id: str = Field(alias="client-id")
    secret: str
    login_url: str = Field(alias="login-url")

    @property
    def auth_header(self) -> dict:
        if self.token:
            return {
                "Authorization": f"Bearer {self.token}"
            }
        else:
            raise ValueError("Token is not available. Please check your credentials or login URL.")

    @property
    def token(self) -> str:
        payload = {
            "clientId": self.client_id,
            "secret": self.secret
        }
        try:
            response = requests.request("POST", self.login_url, json=payload)
            return response.json()["token"]
        except (KeyError, ValueError, ConnectTimeout):
            return None
