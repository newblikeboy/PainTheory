from dataclasses import dataclass
import json
import os
from typing import Any, Dict
from urllib.parse import parse_qs, unquote, urlparse


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_text(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    text = value.strip()
    return text if text else default


def _load_auth_file(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="ascii") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}


def _parse_mysql_uri(uri: str) -> Dict[str, Any]:
    text = str(uri or "").strip()
    if not text:
        return {}
    try:
        parsed = urlparse(text)
    except ValueError:
        return {}
    scheme = str(parsed.scheme or "").strip().lower()
    if scheme not in {"mysql", "mysql2"}:
        return {}
    query = parse_qs(parsed.query, keep_blank_values=False)

    def pick_query(*keys: str) -> str:
        for key in keys:
            values = query.get(key)
            if values:
                value = str(values[0] or "").strip()
                if value:
                    return value
        return ""

    database = str(parsed.path or "").strip().lstrip("/")
    out: Dict[str, Any] = {}
    if parsed.hostname:
        out["host"] = str(parsed.hostname)
    if parsed.port:
        out["port"] = int(parsed.port)
    if parsed.username is not None:
        out["user"] = unquote(str(parsed.username))
    if parsed.password is not None:
        out["password"] = unquote(str(parsed.password))
    if database:
        out["database"] = database
    ssl_mode = pick_query("ssl-mode", "ssl_mode", "sslmode")
    if ssl_mode:
        out["ssl_mode"] = ssl_mode
    ssl_ca = pick_query("ssl-ca", "ssl_ca")
    if ssl_ca:
        out["ssl_ca"] = ssl_ca
    return out


def mysql_connect_kwargs_from_parts(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str = "",
    connect_timeout_sec: int = 5,
    with_database: bool = True,
    autocommit: bool = False,
    ssl_mode: str = "",
    ssl_ca: str = "",
    ssl_disabled: bool = False,
    ssl_verify_cert: bool = False,
    ssl_verify_identity: bool = False,
) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "host": str(host or "127.0.0.1"),
        "port": int(port or 3306),
        "user": str(user or ""),
        "password": str(password or ""),
        "connection_timeout": max(2, int(connect_timeout_sec or 5)),
        "autocommit": bool(autocommit),
    }
    if with_database:
        kwargs["database"] = str(database or "").strip()

    if bool(ssl_disabled):
        kwargs["ssl_disabled"] = True
        return kwargs

    mode = str(ssl_mode or "").strip().lower()
    if mode in {"disabled", "disable", "off", "false", "0"}:
        kwargs["ssl_disabled"] = True
        return kwargs

    use_tls = bool(mode) or bool(str(ssl_ca or "").strip())
    if not use_tls:
        return kwargs

    kwargs["ssl_disabled"] = False
    ca_path = str(ssl_ca or "").strip()
    if ca_path:
        kwargs["ssl_ca"] = ca_path

    mode_verify_cert = mode in {"verify_ca", "verify_identity"}
    mode_verify_identity = mode in {"verify_identity"}
    if bool(ssl_verify_cert) or mode_verify_cert:
        kwargs["ssl_verify_cert"] = True
    if bool(ssl_verify_identity) or mode_verify_identity:
        kwargs["ssl_verify_identity"] = True
    return kwargs


def mysql_connect_kwargs(settings: "Settings", *, with_database: bool = True, autocommit: bool = False) -> Dict[str, Any]:
    return mysql_connect_kwargs_from_parts(
        host=settings.auth_mysql_host,
        port=settings.auth_mysql_port,
        user=settings.auth_mysql_user,
        password=settings.auth_mysql_password,
        database=settings.auth_mysql_database,
        connect_timeout_sec=settings.auth_mysql_connect_timeout_sec,
        with_database=with_database,
        autocommit=autocommit,
        ssl_mode=settings.auth_mysql_ssl_mode,
        ssl_ca=settings.auth_mysql_ssl_ca,
        ssl_disabled=settings.auth_mysql_ssl_disabled,
        ssl_verify_cert=settings.auth_mysql_ssl_verify_cert,
        ssl_verify_identity=settings.auth_mysql_ssl_verify_identity,
    )


@dataclass
class Settings:
    stream_mode: str
    fyers_auth_file: str
    fyers_access_token: str
    fyers_client_id: str
    fyers_symbol: str
    fyers_data_type: str
    fyers_log_path: str
    fyers_lite_mode: bool
    fyers_reconnect: bool
    fyers_volume_mode: str
    fyers_debug: bool
    options_enabled: bool
    options_poll_interval_sec: int
    options_strikecount: int
    fyers_option_symbol: str
    market_hours_only: bool
    pain_ai_use_model: bool
    pain_ai_model_path: str
    model_auto_retrain_enabled: bool
    model_auto_retrain_run_on_start: bool
    model_auto_retrain_interval_sec: int
    model_auto_retrain_daily_time: str
    model_auto_retrain_lookback_days: int
    model_auto_retrain_min_rows: int
    model_auto_retrain_min_test_rows: int
    model_auto_retrain_min_improvement: float
    model_auto_retrain_max_core_drop: float
    model_auto_retrain_window_1m: int
    model_auto_retrain_window_5m: int
    model_auto_retrain_next_horizon: int
    model_auto_retrain_test_size: float
    model_auto_retrain_candidate_path: str
    model_auto_retrain_backup_dir: str
    model_auto_retrain_runs_table: str
    auth_session_ttl_sec: int
    auth_secret: str
    auth_mysql_uri: str
    auth_mysql_host: str
    auth_mysql_port: int
    auth_mysql_user: str
    auth_mysql_password: str
    auth_mysql_database: str
    auth_mysql_connect_timeout_sec: int
    auth_mysql_ssl_mode: str
    auth_mysql_ssl_ca: str
    auth_mysql_ssl_disabled: bool
    auth_mysql_ssl_verify_cert: bool
    auth_mysql_ssl_verify_identity: bool
    angel_api_key: str
    angel_publisher_login: str
    angel_redirect_url: str
    app_host: str
    angel_client_local_ip: str
    angel_client_public_ip: str
    angel_client_mac: str
    central_live_execution_enabled: bool
    central_live_execution_quantity: int
    central_live_execution_exchange: str
    central_live_execution_product_type: str
    central_live_execution_order_type: str
    central_live_execution_variety: str
    central_live_execution_duration: str
    central_live_execution_timeout_sec: int
    razorpay_key_id: str
    razorpay_key_secret: str
    razorpay_currency: str
    razorpay_subscribe_plan_name: str
    razorpay_subscribe_amount_paisa: int
    fyers_secret_key: str
    fyers_pin: str
    fyers_redirect_uri: str
    fyers_response_type: str
    fyers_scope: str
    fyers_refresh_enabled: bool
    fyers_refresh_check_sec: int
    fyers_refresh_lead_sec: int
    runtime_preload_enabled: bool
    runtime_preload_candles: int
    runtime_preload_table_1m: str
    runtime_use_5m_primary: bool
    runtime_persist_live_candles: bool
    runtime_mysql_table_1m: str
    runtime_mysql_table_5m: str
    runtime_persist_option_snapshots: bool
    runtime_mysql_options_table: str
    runtime_mysql_option_strikes_table: str
    db_retention_enabled: bool
    db_retention_run_on_start: bool
    db_retention_interval_sec: int
    db_retention_candles_1m_days: int
    db_retention_candles_5m_days: int
    db_retention_option_snapshots_days: int
    db_retention_option_strikes_days: int
    db_retention_paper_trades_days: int
    db_retention_paper_feedback_days: int
    db_retention_backtest_runs_days: int
    db_retention_backtest_trades_days: int
    db_retention_auth_sessions_days: int
    max_future_timestamp_sec: int
    paper_trade_enabled: bool
    paper_trade_hold_minutes: int
    paper_trade_min_confidence: float
    paper_trade_entry_guidance: str
    paper_trade_one_trade_at_a_time: bool
    paper_trade_require_pain_release: bool
    paper_trade_entry_style: str
    paper_trade_min_compression_bars: int
    paper_trade_target_r_multiple: float
    paper_trade_avoid_noon_chop: bool
    paper_trade_quote_poll_sec: int
    paper_trade_feedback_enabled: bool
    paper_trade_feedback_min_trades: int
    paper_trade_feedback_block_win_rate: float
    paper_trade_feedback_penalty_floor: float
    paper_trade_feedback_reward_cap: float
    paper_trade_mysql_state_table: str
    paper_trade_mysql_trades_table: str
    paper_trade_mysql_feedback_table: str
    paper_trade_mysql_mistakes_table: str
    backtest_mysql_runs_table: str
    backtest_mysql_trades_table: str


def load_settings() -> Settings:
    auth_path = _get_text("FYERS_AUTH_FILE", "data/fyers_auth.json")
    auth_data = _load_auth_file(auth_path)
    mysql_uri = _get_text("AUTH_MYSQL_URI", _get_text("DATABASE_URL", _get_text("MYSQL_URL", "")))
    parsed_mysql = _parse_mysql_uri(mysql_uri)
    if mysql_uri:
        mysql_host = str(parsed_mysql.get("host", "127.0.0.1"))
        mysql_port = int(parsed_mysql.get("port", 3306))
        mysql_user = str(parsed_mysql.get("user", "root"))
        mysql_password = str(parsed_mysql.get("password", ""))
        mysql_database = str(parsed_mysql.get("database", "pain_theory"))
    else:
        mysql_host = _get_text("AUTH_MYSQL_HOST", "127.0.0.1")
        mysql_port = _get_int("AUTH_MYSQL_PORT", 3306)
        mysql_user = _get_text("AUTH_MYSQL_USER", "root")
        mysql_password = _get_text("AUTH_MYSQL_PASSWORD", "")
        mysql_database = _get_text("AUTH_MYSQL_DATABASE", "pain_theory")
    mysql_ssl_mode_default = str(parsed_mysql.get("ssl_mode", "")).strip()
    if not mysql_ssl_mode_default and mysql_uri:
        # Cloud MySQL providers like Aiven generally require TLS.
        mysql_ssl_mode_default = "required"
    return Settings(
        stream_mode=_get_text("STREAM_MODE", "fyers"),
        fyers_auth_file=auth_path,
        fyers_access_token=auth_data.get("access_token", ""),
        fyers_client_id=_get_text("FYERS_CLIENT_ID", auth_data.get("client_id", "")),
        fyers_symbol=_get_text("FYERS_SYMBOL", "NSE:NIFTY50-INDEX"),
        fyers_data_type=_get_text("FYERS_DATA_TYPE", "symbolData"),
        fyers_log_path=_get_text("FYERS_LOG_PATH", "logs/fyers"),
        fyers_lite_mode=_get_bool("FYERS_LITE_MODE", False),
        fyers_reconnect=_get_bool("FYERS_RECONNECT", True),
        fyers_volume_mode=_get_text("FYERS_VOLUME_MODE", "delta"),
        fyers_debug=_get_bool("FYERS_DEBUG", False),
        options_enabled=_get_bool("OPTIONS_ENABLED", True),
        options_poll_interval_sec=_get_int("OPTIONS_POLL_INTERVAL_SEC", 20),
        # Keep enough depth so we can consistently maintain ATM +/- 5 strikes.
        options_strikecount=max(11, _get_int("OPTIONS_STRIKECOUNT", 11)),
        fyers_option_symbol=_get_text("FYERS_OPTION_SYMBOL", "NSE:NIFTY50-INDEX"),
        market_hours_only=_get_bool("MARKET_HOURS_ONLY", True),
        pain_ai_use_model=_get_bool("PAIN_AI_USE_MODEL", True),
        pain_ai_model_path=_get_text("PAIN_AI_MODEL_PATH", "models/pain_theory_ai.pkl"),
        model_auto_retrain_enabled=_get_bool("MODEL_AUTO_RETRAIN_ENABLED", True),
        model_auto_retrain_run_on_start=_get_bool("MODEL_AUTO_RETRAIN_RUN_ON_START", False),
        model_auto_retrain_interval_sec=max(300, _get_int("MODEL_AUTO_RETRAIN_INTERVAL_SEC", 21600)),
        model_auto_retrain_daily_time=_get_text("MODEL_AUTO_RETRAIN_DAILY_TIME", ""),
        model_auto_retrain_lookback_days=max(7, _get_int("MODEL_AUTO_RETRAIN_LOOKBACK_DAYS", 120)),
        model_auto_retrain_min_rows=max(500, _get_int("MODEL_AUTO_RETRAIN_MIN_ROWS", 3000)),
        model_auto_retrain_min_test_rows=max(100, _get_int("MODEL_AUTO_RETRAIN_MIN_TEST_ROWS", 700)),
        model_auto_retrain_min_improvement=max(0.0, _get_float("MODEL_AUTO_RETRAIN_MIN_IMPROVEMENT", 0.002)),
        model_auto_retrain_max_core_drop=max(0.0, _get_float("MODEL_AUTO_RETRAIN_MAX_CORE_DROP", 0.01)),
        model_auto_retrain_window_1m=max(5, _get_int("MODEL_AUTO_RETRAIN_WINDOW_1M", 30)),
        model_auto_retrain_window_5m=max(10, _get_int("MODEL_AUTO_RETRAIN_WINDOW_5M", 30)),
        model_auto_retrain_next_horizon=max(1, _get_int("MODEL_AUTO_RETRAIN_NEXT_HORIZON", 15)),
        model_auto_retrain_test_size=max(
            0.05,
            min(0.5, _get_float("MODEL_AUTO_RETRAIN_TEST_SIZE", 0.2)),
        ),
        model_auto_retrain_candidate_path=_get_text(
            "MODEL_AUTO_RETRAIN_CANDIDATE_PATH",
            "models/pain_theory_ai_candidate.pkl",
        ),
        model_auto_retrain_backup_dir=_get_text("MODEL_AUTO_RETRAIN_BACKUP_DIR", "models/backups"),
        model_auto_retrain_runs_table=_get_text("MODEL_AUTO_RETRAIN_RUNS_TABLE", "model_retrain_runs"),
        auth_session_ttl_sec=_get_int("AUTH_SESSION_TTL_SEC", 86400),
        auth_secret=_get_text("AUTH_SECRET", "pain-theory-dev-secret"),
        auth_mysql_uri=mysql_uri,
        auth_mysql_host=mysql_host,
        auth_mysql_port=mysql_port,
        auth_mysql_user=mysql_user,
        auth_mysql_password=mysql_password,
        auth_mysql_database=mysql_database,
        auth_mysql_connect_timeout_sec=_get_int("AUTH_MYSQL_CONNECT_TIMEOUT_SEC", 5),
        auth_mysql_ssl_mode=_get_text("AUTH_MYSQL_SSL_MODE", mysql_ssl_mode_default),
        auth_mysql_ssl_ca=_get_text("AUTH_MYSQL_SSL_CA", str(parsed_mysql.get("ssl_ca", ""))),
        auth_mysql_ssl_disabled=_get_bool("AUTH_MYSQL_SSL_DISABLED", False),
        auth_mysql_ssl_verify_cert=_get_bool("AUTH_MYSQL_SSL_VERIFY_CERT", False),
        auth_mysql_ssl_verify_identity=_get_bool("AUTH_MYSQL_SSL_VERIFY_IDENTITY", False),
        angel_api_key=_get_text("ANGEL_API_KEY", _get_text("API_KEY", "")),
        angel_publisher_login=_get_text(
            "ANGEL_PUBLISHER_LOGIN",
            "https://smartapi.angelbroking.com/publisher-login/",
        ),
        angel_redirect_url=_get_text("ANGEL_REDIRECT_URL", ""),
        app_host=_get_text("APP_HOST", "http://localhost:8000"),
        angel_client_local_ip=_get_text("CLIENT_LOCAL_IP", "127.0.0.1"),
        angel_client_public_ip=_get_text("CLIENT_PUBLIC_IP", "0.0.0.0"),
        angel_client_mac=_get_text("CLIENT_MAC", "00:00:00:00:00:00"),
        central_live_execution_enabled=_get_bool("CENTRAL_LIVE_EXECUTION_ENABLED", False),
        central_live_execution_quantity=max(1, _get_int("CENTRAL_LIVE_EXECUTION_QUANTITY", 1)),
        central_live_execution_exchange=_get_text("CENTRAL_LIVE_EXECUTION_EXCHANGE", "NFO"),
        central_live_execution_product_type=_get_text("CENTRAL_LIVE_EXECUTION_PRODUCT_TYPE", "INTRADAY"),
        central_live_execution_order_type=_get_text("CENTRAL_LIVE_EXECUTION_ORDER_TYPE", "MARKET"),
        central_live_execution_variety=_get_text("CENTRAL_LIVE_EXECUTION_VARIETY", "NORMAL"),
        central_live_execution_duration=_get_text("CENTRAL_LIVE_EXECUTION_DURATION", "DAY"),
        central_live_execution_timeout_sec=max(5, _get_int("CENTRAL_LIVE_EXECUTION_TIMEOUT_SEC", 20)),
        razorpay_key_id=_get_text("RAZORPAY_KEY_ID", ""),
        razorpay_key_secret=_get_text("RAZORPAY_KEY_SECRET", ""),
        razorpay_currency=_get_text("RAZORPAY_CURRENCY", "INR"),
        razorpay_subscribe_plan_name=_get_text("RAZORPAY_SUBSCRIBE_PLAN_NAME", "Sensible Algo Pro"),
        razorpay_subscribe_amount_paisa=max(100, _get_int("RAZORPAY_SUBSCRIBE_AMOUNT_PAISA", 49900)),
        fyers_secret_key=_get_text("FYERS_SECRET_KEY", ""),
        fyers_pin=_get_text("FYERS_PIN", ""),
        fyers_redirect_uri=_get_text("FYERS_REDIRECT_URI", ""),
        fyers_response_type=_get_text("FYERS_RESPONSE_TYPE", "code"),
        fyers_scope=_get_text("FYERS_SCOPE", ""),
        fyers_refresh_enabled=_get_bool("FYERS_REFRESH_ENABLED", True),
        fyers_refresh_check_sec=_get_int("FYERS_REFRESH_CHECK_SEC", 30),
        fyers_refresh_lead_sec=_get_int("FYERS_REFRESH_LEAD_SEC", 300),
        runtime_preload_enabled=_get_bool("RUNTIME_PRELOAD_ENABLED", True),
        runtime_preload_candles=_get_int("RUNTIME_PRELOAD_CANDLES", 300),
        runtime_preload_table_1m=_get_text("RUNTIME_PRELOAD_TABLE_1M", "candles_1m"),
        runtime_use_5m_primary=_get_bool(
            "RUNTIME_USE_5M_PRIMARY",
            _get_bool("RUNTIME_USE_5M_CONFIRMATION", True),
        ),
        runtime_persist_live_candles=_get_bool("RUNTIME_PERSIST_LIVE_CANDLES", True),
        runtime_mysql_table_1m=_get_text("RUNTIME_MYSQL_TABLE_1M", "candles_1m"),
        runtime_mysql_table_5m=_get_text("RUNTIME_MYSQL_TABLE_5M", "candles_5m"),
        runtime_persist_option_snapshots=_get_bool("RUNTIME_PERSIST_OPTION_SNAPSHOTS", True),
        runtime_mysql_options_table=_get_text("RUNTIME_MYSQL_OPTIONS_TABLE", "option_snapshots"),
        runtime_mysql_option_strikes_table=_get_text(
            "RUNTIME_MYSQL_OPTION_STRIKES_TABLE",
            "option_chain_strikes",
        ),
        db_retention_enabled=_get_bool("DB_RETENTION_ENABLED", True),
        db_retention_run_on_start=_get_bool("DB_RETENTION_RUN_ON_START", True),
        db_retention_interval_sec=max(300, _get_int("DB_RETENTION_INTERVAL_SEC", 3600)),
        db_retention_candles_1m_days=max(0, _get_int("DB_RETENTION_CANDLES_1M_DAYS", 30)),
        db_retention_candles_5m_days=max(0, _get_int("DB_RETENTION_CANDLES_5M_DAYS", 60)),
        db_retention_option_snapshots_days=max(0, _get_int("DB_RETENTION_OPTION_SNAPSHOTS_DAYS", 14)),
        db_retention_option_strikes_days=max(0, _get_int("DB_RETENTION_OPTION_STRIKES_DAYS", 7)),
        db_retention_paper_trades_days=max(0, _get_int("DB_RETENTION_PAPER_TRADES_DAYS", 60)),
        db_retention_paper_feedback_days=max(0, _get_int("DB_RETENTION_PAPER_FEEDBACK_DAYS", 120)),
        db_retention_backtest_runs_days=max(0, _get_int("DB_RETENTION_BACKTEST_RUNS_DAYS", 30)),
        db_retention_backtest_trades_days=max(0, _get_int("DB_RETENTION_BACKTEST_TRADES_DAYS", 30)),
        db_retention_auth_sessions_days=max(0, _get_int("DB_RETENTION_AUTH_SESSIONS_DAYS", 7)),
        max_future_timestamp_sec=max(0, _get_int("MAX_FUTURE_TIMESTAMP_SEC", 300)),
        paper_trade_enabled=_get_bool("PAPER_TRADE_ENABLED", True),
        paper_trade_hold_minutes=_get_int("PAPER_TRADE_HOLD_MINUTES", 15),
        paper_trade_min_confidence=_get_float("PAPER_TRADE_MIN_CONFIDENCE", 0.55),
        paper_trade_entry_guidance=_get_text("PAPER_TRADE_ENTRY_GUIDANCE", "caution"),
        paper_trade_one_trade_at_a_time=_get_bool("PAPER_TRADE_ONE_TRADE_AT_A_TIME", True),
        paper_trade_require_pain_release=_get_bool("PAPER_TRADE_REQUIRE_PAIN_RELEASE", False),
        paper_trade_entry_style=_get_text("PAPER_TRADE_ENTRY_STYLE", "aggressive"),
        paper_trade_min_compression_bars=max(2, _get_int("PAPER_TRADE_MIN_COMPRESSION_BARS", 3)),
        paper_trade_target_r_multiple=max(1.0, _get_float("PAPER_TRADE_TARGET_R_MULTIPLE", 1.5)),
        paper_trade_avoid_noon_chop=_get_bool("PAPER_TRADE_AVOID_NOON_CHOP", True),
        paper_trade_quote_poll_sec=max(1, _get_int("PAPER_TRADE_QUOTE_POLL_SEC", 3)),
        paper_trade_feedback_enabled=_get_bool("PAPER_TRADE_FEEDBACK_ENABLED", True),
        paper_trade_feedback_min_trades=max(3, _get_int("PAPER_TRADE_FEEDBACK_MIN_TRADES", 8)),
        paper_trade_feedback_block_win_rate=max(
            0.05,
            min(0.60, _get_float("PAPER_TRADE_FEEDBACK_BLOCK_WIN_RATE", 0.28)),
        ),
        paper_trade_feedback_penalty_floor=max(
            0.35,
            min(0.95, _get_float("PAPER_TRADE_FEEDBACK_PENALTY_FLOOR", 0.55)),
        ),
        paper_trade_feedback_reward_cap=max(
            1.0,
            min(1.5, _get_float("PAPER_TRADE_FEEDBACK_REWARD_CAP", 1.12)),
        ),
        paper_trade_mysql_state_table=_get_text("PAPER_TRADE_MYSQL_STATE_TABLE", "paper_trade_state"),
        paper_trade_mysql_trades_table=_get_text("PAPER_TRADE_MYSQL_TRADES_TABLE", "paper_trade_trades"),
        paper_trade_mysql_feedback_table=_get_text("PAPER_TRADE_MYSQL_FEEDBACK_TABLE", "paper_trade_feedback"),
        paper_trade_mysql_mistakes_table=_get_text("PAPER_TRADE_MYSQL_MISTAKES_TABLE", "paper_trade_mistakes"),
        backtest_mysql_runs_table=_get_text("BACKTEST_MYSQL_RUNS_TABLE", "backtest_runs"),
        backtest_mysql_trades_table=_get_text("BACKTEST_MYSQL_TRADES_TABLE", "backtest_trades"),
    )
