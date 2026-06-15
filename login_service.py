from __future__ import annotations

import hashlib
import hmac
import os
from dataclasses import dataclass


DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "pipepulse-demo"
USERNAME_ENV = "PIPEPULSE_USERNAME"
PASSWORD_ENV = "PIPEPULSE_PASSWORD"
PASSWORD_HASH_ENV = "PIPEPULSE_PASSWORD_HASH"


@dataclass(frozen=True)
class LoginResult:
    authenticated: bool
    username: str | None = None
    message: str = ""


class LoginService:
    """Small environment-backed login service for the Streamlit dashboard."""

    def __init__(self) -> None:
        self.expected_username = (
            os.environ.get(USERNAME_ENV, DEFAULT_USERNAME).strip()
            or DEFAULT_USERNAME
        )
        password = os.environ.get(PASSWORD_ENV, DEFAULT_PASSWORD)
        configured_hash = os.environ.get(PASSWORD_HASH_ENV, "").strip()
        self.expected_password_hash = configured_hash or self.hash_password(password)
        self.uses_default_credentials = (
            self.expected_username == DEFAULT_USERNAME
            and password == DEFAULT_PASSWORD
            and not configured_hash
        )

    @staticmethod
    def hash_password(password: str) -> str:
        return hashlib.sha256(password.encode("utf-8")).hexdigest()

    def authenticate(self, username: str, password: str) -> LoginResult:
        username = username.strip()
        if not username or not password:
            return LoginResult(False, message="Enter both username and password.")

        username_matches = hmac.compare_digest(username, self.expected_username)
        password_matches = hmac.compare_digest(
            self.hash_password(password),
            self.expected_password_hash,
        )
        if username_matches and password_matches:
            return LoginResult(True, username=username)

        return LoginResult(False, message="Invalid username or password.")
