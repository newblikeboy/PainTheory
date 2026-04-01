from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fyers_apiv3 import fyersModel
import requests

from .config import mysql_connect_kwargs_from_parts


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _now() -> int:
    return int(time.time())


def _read_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default)


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _jwt_claim(token: str, key: str, default: int = 0) -> int:
    parts = str(token or "").split(".")
    if len(parts) < 2:
        return default
    payload = parts[1]
    padding = "=" * ((4 - len(payload) % 4) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload + padding)
        obj = json.loads(decoded.decode("utf-8"))
        return _to_int(obj.get(key), default)
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return default


class UserAuthStore:
    _EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    _MOBILE_PATTERN = re.compile(r"^\+?[0-9]{10,15}$")

    def __init__(
        self,
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        raise RuntimeError("File-based auth backend has been removed. Use MySQLUserAuthStore.")

    @staticmethod
    def _normalize_email(email: str) -> str:
        return str(email or "").strip().lower()

    @staticmethod
    def _normalize_mobile(mobile_number: str) -> str:
        text = str(mobile_number or "").strip().replace(" ", "")
        if text.startswith("+"):
            return "+" + re.sub(r"[^0-9]", "", text[1:])
        return re.sub(r"[^0-9]", "", text)

    @staticmethod
    def _hash_password(password: str, salt_hex: str) -> str:
        try:
            salt_bytes = bytes.fromhex(salt_hex)
        except ValueError:
            return ""
        derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt_bytes, 200_000)
        return derived.hex()

    @staticmethod
    def _session_hash(token: str, secret: bytes) -> str:
        return hmac.new(secret, token.encode("utf-8"), hashlib.sha256).hexdigest()

    def _extract_token(self, authorization: str) -> str:
        value = str(authorization or "").strip()
        if not value:
            return ""
        if value.lower().startswith("bearer "):
            return value[7:].strip()
        return ""


class MySQLUserAuthStore(UserAuthStore):
    _VALID_ROLES = {"user", "admin"}

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        secret: str,
        session_ttl_sec: int = 86400,
        connect_timeout_sec: int = 5,
        ssl_mode: str = "",
        ssl_ca: str = "",
        ssl_disabled: bool = False,
        ssl_verify_cert: bool = False,
        ssl_verify_identity: bool = False,
    ) -> None:
        try:
            import mysql.connector  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "mysql-connector-python is required for database auth storage. "
                "Install dependencies from requirements.txt."
            ) from exc

        self._mysql = mysql.connector
        self.host = str(host or "127.0.0.1")
        self.port = int(port or 3306)
        self.user = str(user or "")
        self.password = str(password or "")
        self.database = str(database or "").strip()
        self.connect_timeout_sec = max(2, int(connect_timeout_sec))
        self.ssl_mode = str(ssl_mode or "").strip()
        self.ssl_ca = str(ssl_ca or "").strip()
        self.ssl_disabled = bool(ssl_disabled)
        self.ssl_verify_cert = bool(ssl_verify_cert)
        self.ssl_verify_identity = bool(ssl_verify_identity)
        self.secret = secret.encode("utf-8")
        self.session_ttl_sec = max(300, int(session_ttl_sec))
        self._lock = threading.Lock()

        if not self.database or not re.fullmatch(r"[a-zA-Z0-9_]+", self.database):
            raise RuntimeError("AUTH_MYSQL_DATABASE must contain only letters, numbers, and underscores")
        if not self.user:
            raise RuntimeError("AUTH_MYSQL_USER is required")
        self._initialize_schema()

    def _connect(self, with_database: bool = True):
        kwargs = mysql_connect_kwargs_from_parts(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            connect_timeout_sec=self.connect_timeout_sec,
            with_database=with_database,
            autocommit=False,
            ssl_mode=self.ssl_mode,
            ssl_ca=self.ssl_ca,
            ssl_disabled=self.ssl_disabled,
            ssl_verify_cert=self.ssl_verify_cert,
            ssl_verify_identity=self.ssl_verify_identity,
        )
        return self._mysql.connect(**kwargs)

    def _initialize_schema(self) -> None:
        conn = None
        cur = None
        try:
            conn = self._connect(with_database=False)
            cur = conn.cursor()
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{self.database}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            conn.commit()
            cur.close()
            conn.close()

            conn = self._connect(with_database=True)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_users (
                    email VARCHAR(255) PRIMARY KEY,
                    username VARCHAR(255) NOT NULL,
                    full_name VARCHAR(120) NOT NULL,
                    mobile_number VARCHAR(20) NOT NULL UNIQUE,
                    angel_one_client_id VARCHAR(64) NULL,
                    angel_one_api_key VARCHAR(128) NULL,
                    angel_one_pin VARCHAR(64) NULL,
                    angel_one_totp_secret TEXT NULL,
                    angel_one_connected TINYINT(1) NOT NULL DEFAULT 0,
                    trading_engine_enabled TINYINT(1) NOT NULL DEFAULT 0,
                    user_lot_count INT NOT NULL DEFAULT 1,
                    subscription_active TINYINT(1) NOT NULL DEFAULT 0,
                    subscription_plan_code VARCHAR(32) NULL,
                    subscription_plan_name VARCHAR(120) NULL,
                    subscription_started_at BIGINT NULL,
                    subscription_expires_at BIGINT NULL,
                    subscription_payment_provider VARCHAR(32) NULL,
                    subscription_payment_order_id VARCHAR(80) NULL,
                    subscription_payment_id VARCHAR(80) NULL,
                    subscription_updated_at BIGINT NULL,
                    angel_one_auth_token TEXT NULL,
                    angel_one_access_token TEXT NULL,
                    angel_one_feed_token TEXT NULL,
                    angel_one_refresh_token TEXT NULL,
                    angel_one_token_expires_at BIGINT NULL,
                    angel_one_exchanged_at BIGINT NULL,
                    role VARCHAR(16) NOT NULL DEFAULT 'user',
                    password_salt VARCHAR(64) NOT NULL,
                    password_hash VARCHAR(128) NOT NULL,
                    created_at BIGINT NOT NULL,
                    last_login_at BIGINT NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            self._ensure_user_role_column(conn)
            self._ensure_user_broker_columns(conn)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_sessions (
                    token_hash VARCHAR(128) PRIMARY KEY,
                    username VARCHAR(255) NOT NULL,
                    created_at BIGINT NOT NULL,
                    expires_at BIGINT NOT NULL,
                    revoked TINYINT(1) NOT NULL DEFAULT 0,
                    revoked_at BIGINT NULL,
                    INDEX idx_auth_sessions_username (username),
                    INDEX idx_auth_sessions_expires (expires_at),
                    CONSTRAINT fk_auth_sessions_user
                        FOREIGN KEY (username) REFERENCES auth_users(email)
                        ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_app_settings (
                    setting_key VARCHAR(64) PRIMARY KEY,
                    setting_value VARCHAR(255) NOT NULL,
                    updated_at BIGINT NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute(
                """
                INSERT INTO auth_app_settings (setting_key, setting_value, updated_at)
                VALUES ('lot_size_qty', '1', %s)
                ON DUPLICATE KEY UPDATE
                    setting_value = IF(
                        setting_value IS NULL OR TRIM(setting_value) = '',
                        VALUES(setting_value),
                        setting_value
                    ),
                    updated_at = IF(
                        setting_value IS NULL OR TRIM(setting_value) = '',
                        VALUES(updated_at),
                        updated_at
                    )
                """,
                (_now(),),
            )
            conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    @staticmethod
    def _normalize_role(value: Any, default: str = "user", allow_empty: bool = False) -> str:
        text = str(value or "").strip().lower()
        if allow_empty and not text:
            return ""
        if text in {"user", "admin"}:
            return text
        return str(default or "user").strip().lower() or "user"

    def _ensure_user_role_column(self, conn: Any) -> None:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = 'auth_users'
                  AND COLUMN_NAME = 'role'
                LIMIT 1
                """,
                (self.database,),
            )
            has_role = cur.fetchone() is not None
            if not has_role:
                cur.execute(
                    """
                    ALTER TABLE auth_users
                    ADD COLUMN role VARCHAR(16) NOT NULL DEFAULT 'user'
                    AFTER mobile_number
                    """
                )
            cur.execute(
                """
                UPDATE auth_users
                SET role = 'user'
                WHERE role IS NULL OR TRIM(LOWER(role)) NOT IN ('user', 'admin')
                """
            )
            cur.execute("SELECT COUNT(*) FROM auth_users WHERE LOWER(role) = 'admin'")
            row = cur.fetchone()
            admin_count = _to_int(row[0] if isinstance(row, (tuple, list)) and row else row, 0)
            if admin_count <= 0:
                cur.execute("SELECT email FROM auth_users ORDER BY created_at ASC, email ASC LIMIT 1")
                first = cur.fetchone()
                first_email = ""
                if isinstance(first, (tuple, list)) and first:
                    first_email = str(first[0] or "").strip().lower()
                if first_email:
                    cur.execute("UPDATE auth_users SET role = 'admin' WHERE email = %s", (first_email,))
        finally:
            cur.close()

    def _assign_signup_role(self, conn: Any) -> str:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT COUNT(*) AS c FROM auth_users")
            row = cur.fetchone() or {}
            total_users = _to_int(row.get("c"), 0)
            # Bootstrap convenience: first registered account becomes admin.
            if total_users <= 0:
                return "admin"
            return "user"
        finally:
            cur.close()

    def _ensure_user_broker_columns(self, conn: Any) -> None:
        def ensure_column(column_name: str, alter_sql: str) -> None:
            cur.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = 'auth_users'
                  AND COLUMN_NAME = %s
                LIMIT 1
                """,
                (self.database, column_name),
            )
            has_col = cur.fetchone() is not None
            if not has_col:
                cur.execute(alter_sql)

        cur = conn.cursor()
        try:
            ensure_column(
                "angel_one_client_id",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_client_id VARCHAR(64) NULL
                AFTER mobile_number
                """,
            )
            ensure_column(
                "angel_one_connected",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_connected TINYINT(1) NOT NULL DEFAULT 0
                AFTER angel_one_client_id
                """,
            )
            ensure_column(
                "angel_one_api_key",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_api_key VARCHAR(128) NULL
                AFTER angel_one_client_id
                """,
            )
            ensure_column(
                "angel_one_pin",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_pin VARCHAR(64) NULL
                AFTER angel_one_api_key
                """,
            )
            ensure_column(
                "angel_one_totp_secret",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_totp_secret TEXT NULL
                AFTER angel_one_pin
                """,
            )
            ensure_column(
                "angel_one_auth_token",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_auth_token TEXT NULL
                AFTER angel_one_connected
                """,
            )
            ensure_column(
                "trading_engine_enabled",
                """
                ALTER TABLE auth_users
                ADD COLUMN trading_engine_enabled TINYINT(1) NOT NULL DEFAULT 0
                AFTER angel_one_connected
                """,
            )
            ensure_column(
                "user_lot_count",
                """
                ALTER TABLE auth_users
                ADD COLUMN user_lot_count INT NOT NULL DEFAULT 1
                AFTER trading_engine_enabled
                """,
            )
            ensure_column(
                "subscription_active",
                """
                ALTER TABLE auth_users
                ADD COLUMN subscription_active TINYINT(1) NOT NULL DEFAULT 0
                AFTER user_lot_count
                """,
            )
            ensure_column(
                "subscription_plan_code",
                """
                ALTER TABLE auth_users
                ADD COLUMN subscription_plan_code VARCHAR(32) NULL
                AFTER subscription_active
                """,
            )
            ensure_column(
                "subscription_plan_name",
                """
                ALTER TABLE auth_users
                ADD COLUMN subscription_plan_name VARCHAR(120) NULL
                AFTER subscription_plan_code
                """,
            )
            ensure_column(
                "subscription_started_at",
                """
                ALTER TABLE auth_users
                ADD COLUMN subscription_started_at BIGINT NULL
                AFTER subscription_plan_name
                """,
            )
            ensure_column(
                "subscription_expires_at",
                """
                ALTER TABLE auth_users
                ADD COLUMN subscription_expires_at BIGINT NULL
                AFTER subscription_started_at
                """,
            )
            ensure_column(
                "subscription_payment_provider",
                """
                ALTER TABLE auth_users
                ADD COLUMN subscription_payment_provider VARCHAR(32) NULL
                AFTER subscription_expires_at
                """,
            )
            ensure_column(
                "subscription_payment_order_id",
                """
                ALTER TABLE auth_users
                ADD COLUMN subscription_payment_order_id VARCHAR(80) NULL
                AFTER subscription_payment_provider
                """,
            )
            ensure_column(
                "subscription_payment_id",
                """
                ALTER TABLE auth_users
                ADD COLUMN subscription_payment_id VARCHAR(80) NULL
                AFTER subscription_payment_order_id
                """,
            )
            ensure_column(
                "subscription_updated_at",
                """
                ALTER TABLE auth_users
                ADD COLUMN subscription_updated_at BIGINT NULL
                AFTER subscription_payment_id
                """,
            )
            ensure_column(
                "angel_one_access_token",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_access_token TEXT NULL
                AFTER angel_one_auth_token
                """,
            )
            ensure_column(
                "angel_one_feed_token",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_feed_token TEXT NULL
                AFTER angel_one_access_token
                """,
            )
            ensure_column(
                "angel_one_refresh_token",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_refresh_token TEXT NULL
                AFTER angel_one_feed_token
                """,
            )
            ensure_column(
                "angel_one_token_expires_at",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_token_expires_at BIGINT NULL
                AFTER angel_one_refresh_token
                """,
            )
            ensure_column(
                "angel_one_exchanged_at",
                """
                ALTER TABLE auth_users
                ADD COLUMN angel_one_exchanged_at BIGINT NULL
                AFTER angel_one_token_expires_at
                """,
            )
            cur.execute(
                """
                UPDATE auth_users
                SET angel_one_connected = 0
                WHERE angel_one_connected IS NULL
                """
            )
            cur.execute(
                """
                UPDATE auth_users
                SET trading_engine_enabled = 0
                WHERE trading_engine_enabled IS NULL
                """
            )
            cur.execute(
                """
                UPDATE auth_users
                SET user_lot_count = 1
                WHERE user_lot_count IS NULL OR user_lot_count <= 0
                """
            )
            cur.execute(
                """
                UPDATE auth_users
                SET subscription_active = 0
                WHERE subscription_active IS NULL
                """
            )
        finally:
            cur.close()

    @staticmethod
    def _normalize_angel_client_id(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if len(text) > 64:
            raise ValueError("client id must be at most 64 characters")
        if not re.fullmatch(r"[A-Za-z0-9._\-]+", text):
            raise ValueError("client id contains invalid characters")
        return text

    @staticmethod
    def _normalize_angel_api_key(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if len(text) > 128:
            raise ValueError("api key must be at most 128 characters")
        if not re.fullmatch(r"[A-Za-z0-9._\-]+", text):
            raise ValueError("api key contains invalid characters")
        return text

    @staticmethod
    def _normalize_angel_pin(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if len(text) > 64:
            raise ValueError("pin must be at most 64 characters")
        return text

    @staticmethod
    def _normalize_angel_totp_secret(value: str) -> str:
        text = re.sub(r"\s+", "", str(value or "").strip()).upper()
        if not text:
            return ""
        if len(text) > 256:
            raise ValueError("totp secret must be at most 256 characters")
        if not re.fullmatch(r"[A-Z2-7=]+", text):
            raise ValueError("totp secret contains invalid characters")
        return text

    @staticmethod
    def _mask_secret(value: Any, keep: int = 4) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if len(text) <= keep:
            return "*" * len(text)
        return ("*" * max(0, len(text) - keep)) + text[-keep:]

    @staticmethod
    def _normalize_user_lot_count(value: Any) -> int:
        lot_count = _to_int(value, 1)
        if lot_count <= 0:
            raise ValueError("lot_count must be at least 1")
        if lot_count > 1000:
            raise ValueError("lot_count must be at most 1000")
        return lot_count

    @staticmethod
    def _normalize_lot_size_qty(value: Any) -> int:
        lot_size = _to_int(value, 1)
        if lot_size <= 0:
            raise ValueError("lot_size_qty must be at least 1")
        if lot_size > 100000:
            raise ValueError("lot_size_qty must be at most 100000")
        return lot_size

    def get_global_lot_size_qty(self) -> int:
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT setting_value
                    FROM auth_app_settings
                    WHERE setting_key = 'lot_size_qty'
                    LIMIT 1
                    """
                )
                row = cur.fetchone() or {}
                conn.commit()
                return self._normalize_lot_size_qty(row.get("setting_value"))
            except Exception:
                return 1
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def set_global_lot_size_qty(self, lot_size_qty: Any) -> Dict[str, Any]:
        lot_size = self._normalize_lot_size_qty(lot_size_qty)
        now = _now()
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO auth_app_settings (setting_key, setting_value, updated_at)
                    VALUES ('lot_size_qty', %s, %s)
                    ON DUPLICATE KEY UPDATE
                        setting_value = VALUES(setting_value),
                        updated_at = VALUES(updated_at)
                    """,
                    (str(lot_size), now),
                )
                conn.commit()
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()
        return {
            "lot_size_qty": lot_size,
            "updated_at": now,
        }

    def get_user_lot_config(self, username: str) -> Dict[str, Any]:
        normalized = self._normalize_email(username)
        if not normalized:
            raise ValueError("username is required")
        lot_size_qty = self.get_global_lot_size_qty()
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT email, user_lot_count
                    FROM auth_users
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
                conn.commit()
                if not isinstance(row, dict):
                    raise ValueError("user not found")
                user_lot_count = self._normalize_user_lot_count(row.get("user_lot_count"))
                return {
                    "username": self._to_row_value(row, "email", normalized),
                    "user_lot_count": user_lot_count,
                    "lot_size_qty": lot_size_qty,
                    "order_quantity": int(lot_size_qty) * int(user_lot_count),
                }
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def set_user_lot_count(self, username: str, lot_count: Any) -> Dict[str, Any]:
        normalized = self._normalize_email(username)
        if not normalized:
            raise ValueError("username is required")
        user_lot_count = self._normalize_user_lot_count(lot_count)
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE auth_users
                    SET user_lot_count = %s
                    WHERE email = %s
                    """,
                    (int(user_lot_count), normalized),
                )
                if int(cur.rowcount or 0) <= 0:
                    raise ValueError("user not found")
                conn.commit()
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()
        return self.get_user_lot_config(normalized)

    @staticmethod
    def _is_paid_active(row: Dict[str, Any], now_ts: int = 0) -> bool:
        now_value = _to_int(now_ts, _now())
        flag = _to_int((row or {}).get("subscription_active"), 0) > 0
        expires_at = _to_int((row or {}).get("subscription_expires_at"), 0)
        if not flag:
            return False
        if expires_at <= 0:
            return False
        return expires_at > now_value

    @staticmethod
    def _to_row_value(row: Dict[str, Any], key: str, default: str = "") -> str:
        value = row.get(key)
        if value is None:
            return default
        return str(value)

    def _cleanup_sessions(self, conn: Any) -> None:
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM auth_sessions WHERE expires_at <= %s OR revoked = 1",
                (_now(),),
            )
        finally:
            cur.close()

    def signup(
        self,
        full_name: str,
        email: str,
        mobile_number: str,
        password: str,
        confirm_password: str = "",
    ) -> Dict[str, Any]:
        normalized_email = self._normalize_email(email)
        normalized_mobile = self._normalize_mobile(mobile_number)
        full_name = str(full_name or "").strip()
        if len(full_name) < 2:
            raise ValueError("full_name is required")
        if not self._EMAIL_PATTERN.fullmatch(normalized_email):
            raise ValueError("email is invalid")
        if not self._MOBILE_PATTERN.fullmatch(normalized_mobile):
            raise ValueError("mobile_number must be 10-15 digits (optional + prefix)")
        if len(password) < 8:
            raise ValueError("password must be at least 8 characters")
        if confirm_password and password != confirm_password:
            raise ValueError("password and confirm_password do not match")

        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute("SELECT email FROM auth_users WHERE email = %s LIMIT 1", (normalized_email,))
                if cur.fetchone():
                    raise ValueError("email already exists")
                cur.execute("SELECT email FROM auth_users WHERE mobile_number = %s LIMIT 1", (normalized_mobile,))
                if cur.fetchone():
                    raise ValueError("mobile number already exists")

                salt = secrets.token_hex(16)
                now = _now()
                role = self._assign_signup_role(conn)
                cur.execute(
                    """
                    INSERT INTO auth_users (
                        email, username, full_name, mobile_number,
                        role, password_salt, password_hash, created_at, last_login_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        normalized_email,
                        normalized_email,
                        full_name,
                        normalized_mobile,
                        role,
                        salt,
                        self._hash_password(password, salt),
                        now,
                        0,
                    ),
                )
                conn.commit()
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

        return {
            "username": normalized_email,
            "email": normalized_email,
            "full_name": full_name,
            "mobile_number": normalized_mobile,
            "role": role,
        }

    def login(self, email_or_username: str, password: str, required_role: str = "") -> Dict[str, Any]:
        normalized = self._normalize_email(email_or_username)
        expected_role = self._normalize_role(required_role, default="", allow_empty=True)
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT email, username, full_name, mobile_number, angel_one_client_id, angel_one_connected,
                           trading_engine_enabled, user_lot_count,
                           subscription_active, subscription_plan_code, subscription_plan_name,
                           subscription_started_at, subscription_expires_at,
                           password_salt, password_hash, created_at, role
                    FROM auth_users
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
                if not isinstance(row, dict):
                    raise ValueError("invalid username or password")

                expected = self._to_row_value(row, "password_hash")
                salt = self._to_row_value(row, "password_salt")
                actual = self._hash_password(password, salt) if salt else ""
                if not expected or not hmac.compare_digest(expected, actual):
                    raise ValueError("invalid username or password")
                role = self._normalize_role(row.get("role"), default="user")
                now = _now()
                paid_active = self._is_paid_active(row, now_ts=now)
                if expected_role == "admin" and role != "admin":
                    raise ValueError("admin login is required")
                if expected_role == "user" and role not in {"user", "admin"}:
                    raise ValueError("user login is required")

                token = "pta_" + secrets.token_urlsafe(36)
                expires_at = now + self.session_ttl_sec
                token_hash = self._session_hash(token, self.secret)

                self._cleanup_sessions(conn)
                cur.execute(
                    """
                    INSERT INTO auth_sessions (token_hash, username, created_at, expires_at, revoked)
                    VALUES (%s, %s, %s, %s, 0)
                    ON DUPLICATE KEY UPDATE
                        username = VALUES(username),
                        created_at = VALUES(created_at),
                        expires_at = VALUES(expires_at),
                        revoked = 0,
                        revoked_at = NULL
                    """,
                    (token_hash, normalized, now, expires_at),
                )
                cur.execute(
                    "UPDATE auth_users SET last_login_at = %s WHERE email = %s",
                    (now, normalized),
                )
                conn.commit()
                return {
                    "access_token": token,
                    "token_type": "bearer",
                    "expires_at": expires_at,
                    "user": {
                        "username": normalized,
                        "email": self._to_row_value(row, "email", normalized),
                        "full_name": self._to_row_value(row, "full_name"),
                        "mobile_number": self._to_row_value(row, "mobile_number"),
                        "angel_one_client_id": self._to_row_value(row, "angel_one_client_id"),
                        "angel_one_connected": _to_int(row.get("angel_one_connected"), 0) > 0,
                        "trading_engine_enabled": _to_int(row.get("trading_engine_enabled"), 0) > 0 and paid_active,
                        "user_lot_count": self._normalize_user_lot_count(row.get("user_lot_count")),
                        "subscription_active": paid_active,
                        "subscription_plan_code": self._to_row_value(row, "subscription_plan_code"),
                        "subscription_started_at": _to_int(row.get("subscription_started_at"), 0),
                        "subscription_expires_at": _to_int(row.get("subscription_expires_at"), 0),
                        "plan_name": self._to_row_value(row, "subscription_plan_name"),
                        "role": role,
                        "created_at": _to_int(row.get("created_at"), 0),
                        "last_login_at": now,
                    },
                }
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def get_current_user(self, authorization: str) -> Optional[Dict[str, Any]]:
        token = self._extract_token(authorization)
        if not token:
            return None
        token_hash = self._session_hash(token, self.secret)

        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                self._cleanup_sessions(conn)
                now = _now()
                cur.execute(
                    """
                    SELECT u.email, u.username, u.full_name, u.mobile_number, u.angel_one_client_id,
                           u.angel_one_connected, u.trading_engine_enabled, u.user_lot_count,
                           u.subscription_active, u.subscription_plan_code, u.subscription_plan_name,
                           u.subscription_started_at, u.subscription_expires_at,
                           u.role, u.created_at, u.last_login_at
                    FROM auth_sessions s
                    JOIN auth_users u ON u.email = s.username
                    WHERE s.token_hash = %s
                      AND s.revoked = 0
                      AND s.expires_at > %s
                    LIMIT 1
                    """,
                    (token_hash, now),
                )
                row = cur.fetchone()
                conn.commit()
                if not isinstance(row, dict):
                    return None
                username = self._to_row_value(row, "username", self._to_row_value(row, "email"))
                paid_active = self._is_paid_active(row, now_ts=now)
                return {
                    "username": username,
                    "email": self._to_row_value(row, "email", username),
                    "full_name": self._to_row_value(row, "full_name"),
                    "mobile_number": self._to_row_value(row, "mobile_number"),
                    "angel_one_client_id": self._to_row_value(row, "angel_one_client_id"),
                    "angel_one_connected": _to_int(row.get("angel_one_connected"), 0) > 0,
                    "trading_engine_enabled": _to_int(row.get("trading_engine_enabled"), 0) > 0 and paid_active,
                    "user_lot_count": self._normalize_user_lot_count(row.get("user_lot_count")),
                    "subscription_active": paid_active,
                    "subscription_plan_code": self._to_row_value(row, "subscription_plan_code"),
                    "subscription_started_at": _to_int(row.get("subscription_started_at"), 0),
                    "subscription_expires_at": _to_int(row.get("subscription_expires_at"), 0),
                    "plan_name": self._to_row_value(row, "subscription_plan_name"),
                    "role": self._normalize_role(row.get("role"), default="user"),
                    "created_at": _to_int(row.get("created_at"), 0),
                    "last_login_at": _to_int(row.get("last_login_at"), 0),
                }
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def get_user_broker_config(self, username: str) -> Dict[str, Any]:
        normalized = self._normalize_email(username)
        if not normalized:
            raise ValueError("username is required")
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT email, angel_one_client_id, angel_one_api_key, angel_one_pin, angel_one_totp_secret,
                           angel_one_connected, trading_engine_enabled, user_lot_count,
                           subscription_active, subscription_plan_code, subscription_plan_name,
                           subscription_started_at, subscription_expires_at,
                           angel_one_exchanged_at
                    FROM auth_users
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
                conn.commit()
                if not isinstance(row, dict):
                    raise ValueError("user not found")
                paid_active = self._is_paid_active(row)
                return {
                    "username": self._to_row_value(row, "email", normalized),
                    "broker": "angel_one",
                    "client_id": self._to_row_value(row, "angel_one_client_id"),
                    "api_key_saved": bool(self._to_row_value(row, "angel_one_api_key")),
                    "api_key_hint": self._mask_secret(self._to_row_value(row, "angel_one_api_key")),
                    "pin_saved": bool(self._to_row_value(row, "angel_one_pin")),
                    "totp_secret_saved": bool(self._to_row_value(row, "angel_one_totp_secret")),
                    "has_login_credentials": bool(
                        self._to_row_value(row, "angel_one_client_id")
                        and self._to_row_value(row, "angel_one_api_key")
                        and self._to_row_value(row, "angel_one_pin")
                        and self._to_row_value(row, "angel_one_totp_secret")
                    ),
                    "connected": _to_int(row.get("angel_one_connected"), 0) > 0,
                    "trading_engine_enabled": _to_int(row.get("trading_engine_enabled"), 0) > 0 and paid_active,
                    "user_lot_count": self._normalize_user_lot_count(row.get("user_lot_count")),
                    "paid_active": paid_active,
                    "subscription_plan_code": self._to_row_value(row, "subscription_plan_code"),
                    "subscription_plan_name": self._to_row_value(row, "subscription_plan_name"),
                    "subscription_started_at": _to_int(row.get("subscription_started_at"), 0),
                    "subscription_expires_at": _to_int(row.get("subscription_expires_at"), 0),
                    "exchanged_at": _to_int(row.get("angel_one_exchanged_at"), 0),
                }
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def get_user_trading_engine_config(self, username: str) -> Dict[str, Any]:
        normalized = self._normalize_email(username)
        if not normalized:
            raise ValueError("username is required")
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT email, trading_engine_enabled, angel_one_connected, angel_one_client_id, user_lot_count,
                           subscription_active, subscription_plan_code, subscription_plan_name,
                           subscription_started_at, subscription_expires_at
                    FROM auth_users
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
                conn.commit()
                if not isinstance(row, dict):
                    raise ValueError("user not found")
                paid_active = self._is_paid_active(row)
                return {
                    "username": self._to_row_value(row, "email", normalized),
                    "enabled": _to_int(row.get("trading_engine_enabled"), 0) > 0 and paid_active,
                    "user_lot_count": self._normalize_user_lot_count(row.get("user_lot_count")),
                    "paid_active": paid_active,
                    "connected": _to_int(row.get("angel_one_connected"), 0) > 0,
                    "client_id": self._to_row_value(row, "angel_one_client_id"),
                    "subscription_plan_code": self._to_row_value(row, "subscription_plan_code"),
                    "subscription_plan_name": self._to_row_value(row, "subscription_plan_name"),
                    "subscription_started_at": _to_int(row.get("subscription_started_at"), 0),
                    "subscription_expires_at": _to_int(row.get("subscription_expires_at"), 0),
                }
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def set_user_trading_engine_enabled(self, username: str, enabled: bool) -> Dict[str, Any]:
        normalized = self._normalize_email(username)
        if not normalized:
            raise ValueError("username is required")
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT email, trading_engine_enabled, angel_one_connected, angel_one_client_id, user_lot_count,
                           subscription_active, subscription_plan_code, subscription_plan_name,
                           subscription_started_at, subscription_expires_at
                    FROM auth_users
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                before = cur.fetchone()
                if not isinstance(before, dict):
                    raise ValueError("user not found")
                paid_active = self._is_paid_active(before)
                target_enabled = bool(enabled)
                if target_enabled and not paid_active:
                    raise ValueError("Trading Engine can be enabled only for paid users.")

                cur.execute(
                    """
                    UPDATE auth_users
                    SET trading_engine_enabled = %s
                    WHERE email = %s
                    """,
                    (1 if target_enabled else 0, normalized),
                )
                if int(cur.rowcount or 0) <= 0:
                    raise ValueError("user not found")
                cur.execute(
                    """
                    SELECT email, trading_engine_enabled, angel_one_connected, angel_one_client_id, user_lot_count,
                           subscription_active, subscription_plan_code, subscription_plan_name,
                           subscription_started_at, subscription_expires_at
                    FROM auth_users
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
                conn.commit()
                if not isinstance(row, dict):
                    raise ValueError("user not found")
                paid_after = self._is_paid_active(row)
                return {
                    "username": self._to_row_value(row, "email", normalized),
                    "enabled": _to_int(row.get("trading_engine_enabled"), 0) > 0 and paid_after,
                    "user_lot_count": self._normalize_user_lot_count(row.get("user_lot_count")),
                    "paid_active": paid_after,
                    "connected": _to_int(row.get("angel_one_connected"), 0) > 0,
                    "client_id": self._to_row_value(row, "angel_one_client_id"),
                    "subscription_plan_code": self._to_row_value(row, "subscription_plan_code"),
                    "subscription_plan_name": self._to_row_value(row, "subscription_plan_name"),
                    "subscription_started_at": _to_int(row.get("subscription_started_at"), 0),
                    "subscription_expires_at": _to_int(row.get("subscription_expires_at"), 0),
                }
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def activate_user_subscription(
        self,
        *,
        username: str,
        plan_code: str,
        plan_name: str,
        duration_days: int,
        provider: str,
        payment_order_id: str,
        payment_id: str,
    ) -> Dict[str, Any]:
        normalized = self._normalize_email(username)
        if not normalized:
            raise ValueError("username is required")
        plan_code_text = str(plan_code or "").strip().lower()
        plan_name_text = str(plan_name or "").strip()
        provider_text = str(provider or "").strip().lower()
        if not plan_code_text:
            raise ValueError("plan_code is required")
        if not plan_name_text:
            raise ValueError("plan_name is required")
        if not provider_text:
            raise ValueError("provider is required")
        days = max(1, int(duration_days or 1))
        now = _now()
        expires_at = now + (days * 86400)

        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    UPDATE auth_users
                    SET subscription_active = 1,
                        subscription_plan_code = %s,
                        subscription_plan_name = %s,
                        subscription_started_at = %s,
                        subscription_expires_at = %s,
                        subscription_payment_provider = %s,
                        subscription_payment_order_id = %s,
                        subscription_payment_id = %s,
                        subscription_updated_at = %s
                    WHERE email = %s
                    """,
                    (
                        plan_code_text,
                        plan_name_text,
                        now,
                        expires_at,
                        provider_text,
                        str(payment_order_id or "").strip() or None,
                        str(payment_id or "").strip() or None,
                        now,
                        normalized,
                    ),
                )
                if int(cur.rowcount or 0) <= 0:
                    raise ValueError("user not found")
                cur.execute(
                    """
                    SELECT email, trading_engine_enabled, subscription_active, subscription_plan_code,
                           subscription_plan_name, subscription_started_at, subscription_expires_at
                    FROM auth_users
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
                conn.commit()
                if not isinstance(row, dict):
                    raise ValueError("user not found")
                paid_active = self._is_paid_active(row)
                return {
                    "username": self._to_row_value(row, "email", normalized),
                    "paid_active": paid_active,
                    "subscription_plan_code": self._to_row_value(row, "subscription_plan_code"),
                    "subscription_plan_name": self._to_row_value(row, "subscription_plan_name"),
                    "subscription_started_at": _to_int(row.get("subscription_started_at"), 0),
                    "subscription_expires_at": _to_int(row.get("subscription_expires_at"), 0),
                    "trading_engine_enabled": _to_int(row.get("trading_engine_enabled"), 0) > 0 and paid_active,
                }
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def set_user_broker_profile(
        self,
        username: str,
        *,
        client_id: str = "",
        api_key: str = "",
        pin: str = "",
        totp_secret: str = "",
    ) -> Dict[str, Any]:
        normalized = self._normalize_email(username)
        if not normalized:
            raise ValueError("username is required")
        normalized_client_id = self._normalize_angel_client_id(client_id)
        normalized_api_key = self._normalize_angel_api_key(api_key)
        normalized_pin = self._normalize_angel_pin(pin)
        normalized_totp_secret = self._normalize_angel_totp_secret(totp_secret)
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT angel_one_client_id, angel_one_api_key, angel_one_pin, angel_one_totp_secret
                    FROM auth_users
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                before = cur.fetchone()
                if not isinstance(before, dict):
                    raise ValueError("user not found")
                merged_client_id = normalized_client_id or self._to_row_value(before, "angel_one_client_id")
                merged_api_key = normalized_api_key or self._to_row_value(before, "angel_one_api_key")
                merged_pin = normalized_pin or self._to_row_value(before, "angel_one_pin")
                merged_totp_secret = normalized_totp_secret or self._to_row_value(before, "angel_one_totp_secret")
                if not merged_client_id:
                    raise ValueError("client id is required")
                cur.close()
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE auth_users
                    SET angel_one_client_id = %s,
                        angel_one_api_key = %s,
                        angel_one_pin = %s,
                        angel_one_totp_secret = %s,
                        angel_one_connected = 0,
                        angel_one_auth_token = NULL,
                        angel_one_access_token = NULL,
                        angel_one_feed_token = NULL,
                        angel_one_refresh_token = NULL,
                        angel_one_token_expires_at = NULL,
                        angel_one_exchanged_at = NULL
                    WHERE email = %s
                    """,
                    (
                        merged_client_id or None,
                        merged_api_key or None,
                        merged_pin or None,
                        merged_totp_secret or None,
                        normalized,
                    ),
                )
                if int(cur.rowcount or 0) <= 0:
                    raise ValueError("user not found")
                conn.commit()
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()
        return self.get_user_broker_config(normalized)

    def set_user_broker_client_id(self, username: str, client_id: str) -> Dict[str, Any]:
        return self.set_user_broker_profile(username, client_id=client_id)

    def save_user_angel_session(
        self,
        *,
        username: str,
        client_id: str,
        auth_token: str,
        access_token: str,
        feed_token: str,
        refresh_token: str,
        token_expires_at: int = 0,
    ) -> Dict[str, Any]:
        normalized = self._normalize_email(username)
        normalized_client_id = self._normalize_angel_client_id(client_id)
        if not normalized:
            raise ValueError("username is required")
        if not normalized_client_id:
            raise ValueError("client id is required")
        if not str(access_token or "").strip():
            raise ValueError("access token is required")

        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor()
                now = _now()
                cur.execute(
                    """
                    UPDATE auth_users
                    SET angel_one_client_id = %s,
                        angel_one_connected = 1,
                        angel_one_auth_token = %s,
                        angel_one_access_token = %s,
                        angel_one_feed_token = %s,
                        angel_one_refresh_token = %s,
                        angel_one_token_expires_at = %s,
                        angel_one_exchanged_at = %s
                    WHERE email = %s
                    """,
                    (
                        normalized_client_id,
                        str(auth_token or "").strip() or None,
                        str(access_token or "").strip(),
                        str(feed_token or "").strip() or None,
                        str(refresh_token or "").strip() or None,
                        _to_int(token_expires_at, 0) or None,
                        now,
                        normalized,
                    ),
                )
                if int(cur.rowcount or 0) <= 0:
                    raise ValueError("user not found")
                conn.commit()
                return {
                    "username": normalized,
                    "broker": "angel_one",
                    "client_id": normalized_client_id,
                    "connected": True,
                    "exchanged_at": now,
                    "token_expires_at": _to_int(token_expires_at, 0),
                }
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def find_username_by_angel_client_id(self, client_id: str) -> str:
        normalized_client_id = self._normalize_angel_client_id(client_id)
        if not normalized_client_id:
            return ""
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT email
                    FROM auth_users
                    WHERE LOWER(TRIM(angel_one_client_id)) = LOWER(TRIM(%s))
                    LIMIT 1
                    """,
                    (normalized_client_id,),
                )
                row = cur.fetchone()
                conn.commit()
                if not isinstance(row, dict):
                    return ""
                return self._to_row_value(row, "email").strip().lower()
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def clear_user_angel_session(self, username: str) -> Dict[str, Any]:
        normalized = self._normalize_email(username)
        if not normalized:
            raise ValueError("username is required")
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    UPDATE auth_users
                    SET angel_one_connected = 0,
                        angel_one_auth_token = NULL,
                        angel_one_access_token = NULL,
                        angel_one_feed_token = NULL,
                        angel_one_refresh_token = NULL,
                        angel_one_token_expires_at = NULL,
                        angel_one_exchanged_at = NULL
                    WHERE email = %s
                    """,
                    (normalized,),
                )
                if int(cur.rowcount or 0) <= 0:
                    raise ValueError("user not found")
                cur.execute(
                    """
                    SELECT email, angel_one_client_id
                    FROM auth_users
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
                conn.commit()
                if not isinstance(row, dict):
                    raise ValueError("user not found")
                return {
                    "username": self._to_row_value(row, "email", normalized),
                    "broker": "angel_one",
                    "client_id": self._to_row_value(row, "angel_one_client_id"),
                    "connected": False,
                    "exchanged_at": 0,
                }
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def get_user_angel_session(self, username: str) -> Dict[str, Any]:
        normalized = self._normalize_email(username)
        if not normalized:
            raise ValueError("username is required")
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT
                        email,
                        angel_one_client_id,
                        angel_one_api_key,
                        angel_one_pin,
                        angel_one_totp_secret,
                        angel_one_connected,
                        trading_engine_enabled,
                        user_lot_count,
                        subscription_active,
                        subscription_plan_code,
                        subscription_plan_name,
                        subscription_started_at,
                        subscription_expires_at,
                        angel_one_auth_token,
                        angel_one_access_token,
                        angel_one_feed_token,
                        angel_one_refresh_token,
                        angel_one_token_expires_at,
                        angel_one_exchanged_at
                    FROM auth_users
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
                conn.commit()
                if not isinstance(row, dict):
                    raise ValueError("user not found")
                paid_active = self._is_paid_active(row)
                return {
                    "username": self._to_row_value(row, "email", normalized),
                    "client_id": self._to_row_value(row, "angel_one_client_id"),
                    "api_key": self._to_row_value(row, "angel_one_api_key"),
                    "pin": self._to_row_value(row, "angel_one_pin"),
                    "totp_secret": self._to_row_value(row, "angel_one_totp_secret"),
                    "connected": _to_int(row.get("angel_one_connected"), 0) > 0,
                    "trading_engine_enabled": _to_int(row.get("trading_engine_enabled"), 0) > 0 and paid_active,
                    "user_lot_count": self._normalize_user_lot_count(row.get("user_lot_count")),
                    "subscription_active": paid_active,
                    "subscription_plan_code": self._to_row_value(row, "subscription_plan_code"),
                    "subscription_plan_name": self._to_row_value(row, "subscription_plan_name"),
                    "subscription_started_at": _to_int(row.get("subscription_started_at"), 0),
                    "subscription_expires_at": _to_int(row.get("subscription_expires_at"), 0),
                    "auth_token": self._to_row_value(row, "angel_one_auth_token"),
                    "access_token": self._to_row_value(row, "angel_one_access_token"),
                    "feed_token": self._to_row_value(row, "angel_one_feed_token"),
                    "refresh_token": self._to_row_value(row, "angel_one_refresh_token"),
                    "token_expires_at": _to_int(row.get("angel_one_token_expires_at"), 0),
                    "exchanged_at": _to_int(row.get("angel_one_exchanged_at"), 0),
                }
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def list_connected_angel_sessions(self, limit: int = 5000) -> List[Dict[str, Any]]:
        max_rows = max(1, min(int(limit), 20000))
        now = _now()
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT
                        email,
                        angel_one_client_id,
                        angel_one_api_key,
                        angel_one_connected,
                        trading_engine_enabled,
                        user_lot_count,
                        subscription_active,
                        subscription_plan_code,
                        subscription_plan_name,
                        subscription_started_at,
                        subscription_expires_at,
                        angel_one_auth_token,
                        angel_one_access_token,
                        angel_one_feed_token,
                        angel_one_refresh_token,
                        angel_one_token_expires_at,
                        angel_one_exchanged_at
                    FROM auth_users
                    WHERE angel_one_connected = 1
                      AND trading_engine_enabled = 1
                      AND subscription_active = 1
                      AND subscription_expires_at IS NOT NULL
                      AND subscription_expires_at > %s
                      AND angel_one_client_id IS NOT NULL
                      AND angel_one_client_id <> ''
                      AND angel_one_access_token IS NOT NULL
                      AND angel_one_access_token <> ''
                    ORDER BY email ASC
                    LIMIT %s
                    """,
                    (now, max_rows),
                )
                rows = cur.fetchall() or []
                conn.commit()
                out: List[Dict[str, Any]] = []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    paid_active = self._is_paid_active(row, now_ts=now)
                    out.append(
                        {
                            "username": self._to_row_value(row, "email"),
                            "client_id": self._to_row_value(row, "angel_one_client_id"),
                            "api_key": self._to_row_value(row, "angel_one_api_key"),
                            "connected": _to_int(row.get("angel_one_connected"), 0) > 0,
                            "trading_engine_enabled": _to_int(row.get("trading_engine_enabled"), 0) > 0 and paid_active,
                            "user_lot_count": self._normalize_user_lot_count(row.get("user_lot_count")),
                            "subscription_active": paid_active,
                            "subscription_plan_code": self._to_row_value(row, "subscription_plan_code"),
                            "subscription_plan_name": self._to_row_value(row, "subscription_plan_name"),
                            "subscription_started_at": _to_int(row.get("subscription_started_at"), 0),
                            "subscription_expires_at": _to_int(row.get("subscription_expires_at"), 0),
                            "auth_token": self._to_row_value(row, "angel_one_auth_token"),
                            "access_token": self._to_row_value(row, "angel_one_access_token"),
                            "feed_token": self._to_row_value(row, "angel_one_feed_token"),
                            "refresh_token": self._to_row_value(row, "angel_one_refresh_token"),
                            "token_expires_at": _to_int(row.get("angel_one_token_expires_at"), 0),
                            "exchanged_at": _to_int(row.get("angel_one_exchanged_at"), 0),
                        }
                    )
                return out
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()

    def logout(self, authorization: str) -> bool:
        token = self._extract_token(authorization)
        if not token:
            return False
        token_hash = self._session_hash(token, self.secret)
        conn = None
        cur = None
        with self._lock:
            try:
                conn = self._connect(with_database=True)
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE auth_sessions
                    SET revoked = 1, revoked_at = %s
                    WHERE token_hash = %s AND revoked = 0
                    """,
                    (_now(), token_hash),
                )
                affected = int(cur.rowcount or 0)
                conn.commit()
                return affected > 0
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()


class FyersAuthManager:
    def __init__(
        self,
        auth_file: str,
        client_id: str,
        secret_key: str,
        pin: str,
        redirect_uri: str,
        response_type: str = "code",
        scope: str = "",
        refresh_lead_sec: int = 300,
    ) -> None:
        self.auth_path = Path(auth_file)
        self.client_id = str(client_id or "").strip()
        self.secret_key = str(secret_key or "").strip()
        self.pin = str(pin or "").strip()
        self.redirect_uri = str(redirect_uri or "").strip()
        self.response_type = str(response_type or "code").strip()
        self.scope = str(scope or "").strip()
        self.refresh_lead_sec = max(30, int(refresh_lead_sec))
        self._lock = threading.Lock()
        if not self.auth_path.exists():
            _write_json(self.auth_path, {})

    def _read_auth(self) -> Dict[str, Any]:
        return _read_json(self.auth_path, {})

    def _write_auth(self, data: Dict[str, Any]) -> None:
        _write_json(self.auth_path, data)

    @staticmethod
    def _extract_error(raw: Dict[str, Any]) -> str:
        if not isinstance(raw, dict):
            return "unexpected response from FYERS"
        return str(raw.get("message") or raw.get("error") or "FYERS authentication failed")

    def _session(self, grant_type: str) -> fyersModel.SessionModel:
        return fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            response_type=self.response_type,
            scope=self.scope or None,
            grant_type=grant_type,
        )

    def _app_id_hash(self) -> str:
        return hashlib.sha256(f"{self.client_id}:{self.secret_key}".encode("utf-8")).hexdigest()

    def _persist_tokens(self, raw: Dict[str, Any], previous: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        previous = previous or {}
        code = _to_int(raw.get("code"), 200)
        status = str(raw.get("s", "ok")).lower()
        if status == "error" or code >= 400:
            raise RuntimeError(self._extract_error(raw))

        access_token = str(raw.get("access_token") or previous.get("access_token") or "").strip()
        refresh_token = str(raw.get("refresh_token") or previous.get("refresh_token") or "").strip()
        if not access_token:
            raise RuntimeError("FYERS token response did not include access_token")

        now = _now()
        payload = {
            "client_id": self.client_id or str(previous.get("client_id", "")),
            "access_token": access_token,
            "refresh_token": refresh_token,
            "generated_at": now,
            "access_token_expires_at": _jwt_claim(access_token, "exp", _to_int(previous.get("access_token_expires_at"), 0)),
            "refresh_token_expires_at": _jwt_claim(refresh_token, "exp", _to_int(previous.get("refresh_token_expires_at"), 0)),
            "access_token_issued_at": _jwt_claim(access_token, "iat", _to_int(previous.get("access_token_issued_at"), now)),
            "refresh_token_issued_at": _jwt_claim(refresh_token, "iat", _to_int(previous.get("refresh_token_issued_at"), 0)),
            "raw": raw,
        }
        self._write_auth(payload)
        return payload

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            auth = self._read_auth()
            now = _now()
            access_exp = _to_int(auth.get("access_token_expires_at"), 0)
            refresh_exp = _to_int(auth.get("refresh_token_expires_at"), 0)
            has_access_token = bool(auth.get("access_token"))
            has_refresh_token = bool(auth.get("refresh_token"))
            access_valid = bool(access_exp > now + 5)
            refresh_valid = bool(refresh_exp > now + 5)
            return {
                "authenticated": bool(has_access_token and access_valid),
                "has_access_token": has_access_token,
                "has_refresh_token": has_refresh_token,
                "client_id": str(auth.get("client_id") or self.client_id),
                "generated_at": _to_int(auth.get("generated_at"), 0),
                "access_token_expires_at": access_exp,
                "refresh_token_expires_at": refresh_exp,
                "seconds_to_access_expiry": max(0, access_exp - now) if access_exp else 0,
                "seconds_to_refresh_expiry": max(0, refresh_exp - now) if refresh_exp else 0,
                "needs_refresh": bool(has_refresh_token and access_exp and access_exp <= now + self.refresh_lead_sec),
                "refresh_valid": refresh_valid,
            }

    def generate_login_url(self, state: str = "pain-theory-ai") -> str:
        with self._lock:
            if not self.client_id:
                raise RuntimeError("FYERS client_id is missing")
            if not self.redirect_uri:
                raise RuntimeError("FYERS redirect_uri is missing")
            session = self._session(grant_type="authorization_code")
            session.state = state
            return str(session.generate_authcode())

    def exchange_auth_code(self, auth_code: str) -> Dict[str, Any]:
        token = str(auth_code or "").strip()
        if not token:
            raise RuntimeError("auth_code is required")
        with self._lock:
            if not self.secret_key:
                raise RuntimeError("FYERS secret key is missing")
            previous = self._read_auth()
            session = self._session(grant_type="authorization_code")
            session.set_token(token)
            raw = session.generate_token()
            saved = self._persist_tokens(raw, previous=previous)
            saved["authenticated"] = True
            return saved

    def refresh_access_token(self, force: bool = False) -> Dict[str, Any]:
        with self._lock:
            previous = self._read_auth()
            now = _now()
            access_exp = _to_int(previous.get("access_token_expires_at"), 0)
            needs_refresh = bool(access_exp and access_exp <= now + self.refresh_lead_sec)
            if not force and not needs_refresh:
                return {
                    "refreshed": False,
                    "authenticated": bool(access_exp > now + 5),
                    "access_token": str(previous.get("access_token", "")).strip(),
                    "access_token_expires_at": access_exp,
                }

            refresh_token = str(previous.get("refresh_token") or "").strip()
            if not refresh_token:
                raise RuntimeError("refresh_token is missing; re-authenticate with FYERS")
            if not self.secret_key:
                raise RuntimeError("FYERS secret key is missing")
            payload: Dict[str, Any] = {
                "grant_type": "refresh_token",
                "appIdHash": self._app_id_hash(),
                "refresh_token": refresh_token,
            }
            if self.pin:
                payload["pin"] = self.pin
            response = requests.post(
                "https://api-t1.fyers.in/api/v3/validate-refresh-token",
                json=payload,
                timeout=12,
            )
            try:
                raw = response.json()
            except ValueError:
                raw = {
                    "s": "error",
                    "code": int(response.status_code),
                    "message": response.text or "Unable to parse FYERS refresh response",
                }
            saved = self._persist_tokens(raw, previous=previous)
            saved["refreshed"] = True
            saved["authenticated"] = True
            return saved
