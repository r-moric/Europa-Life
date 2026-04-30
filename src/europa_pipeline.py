#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import json
import math
import os
import random
import re
import sqlite3
import statistics
import textwrap
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from string import Template
from typing import Any, Iterable

UTC_NOW = lambda: datetime.now(timezone.utc).replace(microsecond=0).isoformat()
REPO_ROOT = Path(__file__).resolve().parents[1]

CURRENT_LOG_FILE: Path | None = None


def set_log_file(path: Path | None) -> None:
    global CURRENT_LOG_FILE
    CURRENT_LOG_FILE = path


def log_event(message: str) -> None:
    ts = datetime.now().strftime('%H:%M:%S')
    line = f'[{ts}] {message}'
    print(line, flush=True)
    if CURRENT_LOG_FILE is not None:
        CURRENT_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with CURRENT_LOG_FILE.open('a', encoding='utf-8') as fh:
            fh.write(line + '\n')
            fh.flush()

# ---------------------------
# Utilities
# ---------------------------

def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def slugify(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "_", value.strip())
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "item"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def dump_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_prompt_text(relative_path: str, fallback: str) -> str:
    prompt_path = REPO_ROOT / "prompts" / relative_path
    if prompt_path.exists():
        return prompt_path.read_text(encoding="utf-8")
    return textwrap.dedent(fallback).strip() + "\n"


def render_prompt(relative_path: str, fallback: str, **values: Any) -> str:
    template = Template(load_prompt_text(relative_path, fallback))
    normalized = {key: str(value) for key, value in values.items()}
    return template.substitute(normalized).strip() + "\n"


def load_schema_sql() -> str:
    schema_path = REPO_ROOT / "sql" / "schema.sql"
    if schema_path.exists():
        schema_text = schema_path.read_text(encoding="utf-8")
        if "CREATE TABLE IF NOT EXISTS LLM_CALL" in schema_text:
            return schema_text
    return DDL


def save_manifest(root: Path, stage: str, label: str, payload: Any) -> Path:
    manifests_dir = ensure_dir(root / "manifests")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = manifests_dir / f"{stage.lower()}_{slugify(label)}_{stamp}.json"
    dump_json(path, payload)
    return path


def export_query_to_csv(conn: sqlite3.Connection, sql_text: str, out_path: Path, params: Iterable[object] = ()) -> Path:
    ensure_dir(out_path.parent)
    cur = conn.execute(sql_text, tuple(params))
    rows = cur.fetchall()
    fieldnames = [col[0] for col in cur.description]
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(fieldnames)
        for row in rows:
            writer.writerow([row[col] for col in fieldnames])
    return out_path


def export_core_csvs(conn: sqlite3.Connection, root: Path) -> list[str]:
    exports_dir = ensure_dir(root / "exports")
    queries = {
        "part_master.csv": """
            SELECT p.CANONICAL_PART_ID, p.PART_NUMBER, p.CANONICAL_NAME, pt.PART_TYPE_CODE,
                   pc.CATEGORY_CODE AS PART_CATEGORY_CODE, u.UNIT_CODE, ls.STATUS_CODE AS LIFECYCLE_STATUS,
                   p.MASS_KG, p.REVISION
            FROM PART p
            JOIN PART_TYPE pt ON pt.PART_TYPE_ID = p.PART_TYPE_ID
            JOIN PART_CATEGORY pc ON pc.PART_CATEGORY_ID = p.PART_CATEGORY_ID
            JOIN UNIT u ON u.UNIT_ID = p.UNIT_ID
            JOIN LIFECYCLE_STATUS ls ON ls.LIFECYCLE_STATUS_ID = p.LIFECYCLE_STATUS_ID
            ORDER BY p.PART_NUMBER
        """,
        "assembly_master.csv": """
            SELECT a.ASSEMBLY_CODE, p.PART_NUMBER AS ASSEMBLY_PART_NUMBER, p.CANONICAL_NAME AS ASSEMBLY_NAME,
                   a.MISSION_PHASE, a.CRITICALITY, a.PARENT_PROGRAM
            FROM ASSEMBLY a
            JOIN PART p ON p.PART_ID = a.PART_ID
            ORDER BY a.ASSEMBLY_CODE
        """,
        "supply_master.csv": """
            SELECT s.CANONICAL_SUPPLY_ID, s.SUPPLY_CODE, s.CANONICAL_NAME, sc.CATEGORY_CODE AS SUPPLY_CATEGORY_CODE,
                   u.UNIT_CODE, ls.STATUS_CODE AS LIFECYCLE_STATUS, s.HAZMAT_FLAG, s.STORAGE_CLASS
            FROM SUPPLY s
            JOIN SUPPLY_CATEGORY sc ON sc.SUPPLY_CATEGORY_ID = s.SUPPLY_CATEGORY_ID
            JOIN UNIT u ON u.UNIT_ID = s.UNIT_ID
            JOIN LIFECYCLE_STATUS ls ON ls.LIFECYCLE_STATUS_ID = s.LIFECYCLE_STATUS_ID
            ORDER BY s.SUPPLY_CODE
        """,
        "supplier_master.csv": """
            SELECT SUPPLIER_CODE, CANONICAL_NAME, COUNTRY_CODE, SUPPLIER_TIER
            FROM SUPPLIER
            ORDER BY SUPPLIER_CODE
        """,
        "assembly_bom.csv": """
            SELECT a.ASSEMBLY_CODE, parent.PART_NUMBER AS ASSEMBLY_PART_NUMBER, bl.FIND_NUMBER,
                   child.PART_NUMBER AS CHILD_PART_NUMBER, child.CANONICAL_NAME AS CHILD_PART_NAME,
                   bl.QUANTITY, u.UNIT_CODE
            FROM ASSEMBLY_BOM_LINE bl
            JOIN ASSEMBLY a ON a.ASSEMBLY_ID = bl.ASSEMBLY_ID
            JOIN PART parent ON parent.PART_ID = a.PART_ID
            JOIN PART child ON child.PART_ID = bl.CHILD_PART_ID
            JOIN UNIT u ON u.UNIT_ID = bl.UNIT_ID
            ORDER BY a.ASSEMBLY_CODE, bl.FIND_NUMBER
        """,
        "assembly_supply_requirement.csv": """
            SELECT a.ASSEMBLY_CODE, parent.PART_NUMBER AS ASSEMBLY_PART_NUMBER, s.SUPPLY_CODE,
                   s.CANONICAL_NAME AS SUPPLY_NAME, ar.QUANTITY, u.UNIT_CODE, ar.USE_CONTEXT
            FROM ASSEMBLY_SUPPLY_REQUIREMENT ar
            JOIN ASSEMBLY a ON a.ASSEMBLY_ID = ar.ASSEMBLY_ID
            JOIN PART parent ON parent.PART_ID = a.PART_ID
            JOIN SUPPLY s ON s.SUPPLY_ID = ar.SUPPLY_ID
            JOIN UNIT u ON u.UNIT_ID = ar.UNIT_ID
            ORDER BY a.ASSEMBLY_CODE, s.SUPPLY_CODE
        """,
        "resolved_entity_coverage.csv": """
            SELECT gd.PACKAGE_ID, dt.TYPE_CODE AS DOCUMENT_TYPE, gd.DOC_ID, nm.MENTION_TEXT,
                   nr.RESOLVED_ENTITY_KIND, nr.RESOLUTION_METHOD, nr.CONFIDENCE_SCORE, nr.PROVENANCE_STRENGTH,
                   COALESCE(p.PART_NUMBER, s.SUPPLY_CODE, sup.SUPPLIER_CODE, a.ASSEMBLY_CODE) AS RESOLVED_CODE,
                   COALESCE(p.CANONICAL_NAME, s.CANONICAL_NAME, sup.CANONICAL_NAME, ap.CANONICAL_NAME) AS RESOLVED_NAME
            FROM NER_RESOLUTION nr
            JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID
            JOIN GENERATED_DOCUMENT gd ON gd.GENERATED_DOCUMENT_ID = nm.GENERATED_DOCUMENT_ID
            JOIN DOCUMENT_TYPE dt ON dt.DOCUMENT_TYPE_ID = gd.DOCUMENT_TYPE_ID
            LEFT JOIN PART p ON p.PART_ID = nr.PART_ID
            LEFT JOIN SUPPLY s ON s.SUPPLY_ID = nr.SUPPLY_ID
            LEFT JOIN SUPPLIER sup ON sup.SUPPLIER_ID = nr.SUPPLIER_ID
            LEFT JOIN ASSEMBLY a ON a.ASSEMBLY_ID = nr.ASSEMBLY_ID
            LEFT JOIN PART ap ON ap.PART_ID = a.PART_ID
            ORDER BY gd.PACKAGE_ID, gd.DOC_ID, nm.START_OFFSET
        """,
    }
    exported_paths = []
    for filename, sql_text in queries.items():
        exported_paths.append(str(export_query_to_csv(conn, sql_text, exports_dir / filename)))
    return exported_paths


def configure_logging(root: Path, stage: str, label: str) -> Path:
    logs_dir = root / 'logs'
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"{stage.lower()}_{slugify(label)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    set_log_file(log_path)
    log_event(f"LOG: writing to {log_path}")
    return log_path


def weighted_source_mix(mix: dict[str, int], n: int, rng: random.Random) -> list[str]:
    keys = list(mix)
    weights = [max(0, int(mix[k])) for k in keys]
    if sum(weights) == 0:
        weights = [1] * len(keys)
    return rng.choices(keys, weights=weights, k=n)


def title_case_code(prefix: str, i: int, width: int = 3) -> str:
    return f"{prefix}-{i:0{width}d}"


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def score_text_similarity(a: str, b: str) -> float:
    sa = set(re.findall(r"[a-z0-9]+", a.lower()))
    sb = set(re.findall(r"[a-z0-9]+", b.lower()))
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


class OllamaClient:
    def __init__(self, base_url: str, timeout_sec: int = 120, retry_count: int = 0, retry_backoff_sec: int = 5):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.retry_count = retry_count
        self.retry_backoff_sec = retry_backoff_sec

    def generate(self, model: str, prompt: str, temperature: float = 0.2, keep_alive: str = "10m") -> str:
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": temperature},
            "keep_alive": keep_alive,
        }
        req = urllib.request.Request(
            f"{self.base_url}/api/generate",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        last_exc = None
        for attempt in range(self.retry_count + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
                return str(body.get("response", "")).strip()
            except Exception as exc:
                last_exc = exc
                if attempt < self.retry_count:
                    time.sleep(self.retry_backoff_sec * (attempt + 1))
                    continue
                raise RuntimeError(f"Ollama call failed after {attempt + 1} attempt(s): {exc}") from exc
        raise RuntimeError(f"Ollama call failed: {last_exc}")


def log_llm_call(conn: sqlite3.Connection, stage_code: str, role_code: str, model_name: str | None, run_context: str | None = None,
                 generated_document_id: int | None = None, ner_mention_id: int | None = None, audit_case_id: int | None = None,
                 prompt_text: str | None = None, response_text: str | None = None, success_flag: bool = True) -> None:
    conn.execute(
        "INSERT INTO LLM_CALL (STAGE_CODE, ROLE_CODE, MODEL_NAME, RUN_CONTEXT, GENERATED_DOCUMENT_ID, NER_MENTION_ID, AUDIT_CASE_ID, PROMPT_TEXT, RESPONSE_TEXT, SUCCESS_FLAG, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (stage_code, role_code, model_name, run_context, generated_document_id, ner_mention_id, audit_case_id, prompt_text, response_text, 1 if success_flag else 0, UTC_NOW()),
    )


def llm_usage_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    by_role = [dict(r) for r in conn.execute("SELECT ROLE_CODE, COUNT(*) AS CALL_COUNT FROM LLM_CALL GROUP BY ROLE_CODE ORDER BY ROLE_CODE")]
    by_stage = [dict(r) for r in conn.execute("SELECT STAGE_CODE, COUNT(*) AS CALL_COUNT FROM LLM_CALL GROUP BY STAGE_CODE ORDER BY STAGE_CODE")]
    by_model = [dict(r) for r in conn.execute("SELECT COALESCE(MODEL_NAME, 'NONE') AS MODEL_NAME, COUNT(*) AS CALL_COUNT FROM LLM_CALL GROUP BY COALESCE(MODEL_NAME, 'NONE') ORDER BY CALL_COUNT DESC, MODEL_NAME")]
    return {
        "total_calls": conn.execute("SELECT COUNT(*) FROM LLM_CALL").fetchone()[0],
        "successful_calls": conn.execute("SELECT COUNT(*) FROM LLM_CALL WHERE SUCCESS_FLAG = 1").fetchone()[0],
        "by_role": by_role,
        "by_stage": by_stage,
        "by_model": by_model,
    }


def safe_extract_json_block(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def _normalize_scalar(value: Any, default: str | None = None) -> str | None:
    if value is None:
        return default
    if isinstance(value, list):
        flat = []
        for item in value:
            if item is None:
                continue
            if isinstance(item, (dict, list)):
                flat.append(json.dumps(item, ensure_ascii=False))
            else:
                flat.append(str(item))
        return '; '.join(x for x in flat if x).strip() or default
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)

# ---------------------------
# Database
# ---------------------------

DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS UNIT (
    UNIT_ID INTEGER PRIMARY KEY,
    UNIT_CODE TEXT NOT NULL UNIQUE,
    UNIT_NAME TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS LIFECYCLE_STATUS (
    LIFECYCLE_STATUS_ID INTEGER PRIMARY KEY,
    STATUS_CODE TEXT NOT NULL UNIQUE,
    STATUS_NAME TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS PART_TYPE (
    PART_TYPE_ID INTEGER PRIMARY KEY,
    PART_TYPE_CODE TEXT NOT NULL UNIQUE,
    PART_TYPE_NAME TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS PART_CATEGORY (
    PART_CATEGORY_ID INTEGER PRIMARY KEY,
    CATEGORY_CODE TEXT NOT NULL UNIQUE,
    CATEGORY_NAME TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS SUPPLY_CATEGORY (
    SUPPLY_CATEGORY_ID INTEGER PRIMARY KEY,
    CATEGORY_CODE TEXT NOT NULL UNIQUE,
    CATEGORY_NAME TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS DOCUMENT_TYPE (
    DOCUMENT_TYPE_ID INTEGER PRIMARY KEY,
    TYPE_CODE TEXT NOT NULL UNIQUE,
    TYPE_NAME TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ATTRIBUTE_DEFINITION (
    ATTRIBUTE_DEFINITION_ID INTEGER PRIMARY KEY,
    ENTITY_KIND TEXT NOT NULL CHECK (ENTITY_KIND IN ('PART', 'SUPPLY')),
    ATTRIBUTE_CODE TEXT NOT NULL,
    ATTRIBUTE_NAME TEXT NOT NULL,
    VALUE_TYPE TEXT NOT NULL,
    DEFAULT_UNIT_ID INTEGER,
    UNIQUE (ENTITY_KIND, ATTRIBUTE_CODE),
    FOREIGN KEY (DEFAULT_UNIT_ID) REFERENCES UNIT(UNIT_ID)
);

CREATE TABLE IF NOT EXISTS SUPPLIER (
    SUPPLIER_ID INTEGER PRIMARY KEY,
    SUPPLIER_CODE TEXT NOT NULL UNIQUE,
    CANONICAL_NAME TEXT NOT NULL,
    COUNTRY_CODE TEXT,
    SUPPLIER_TIER TEXT,
    NOTES TEXT
);

CREATE TABLE IF NOT EXISTS SUPPLIER_ALIAS (
    SUPPLIER_ALIAS_ID INTEGER PRIMARY KEY,
    SUPPLIER_ID INTEGER NOT NULL,
    ALIAS_TEXT TEXT NOT NULL,
    ALIAS_KIND TEXT NOT NULL,
    UNIQUE (SUPPLIER_ID, ALIAS_TEXT),
    FOREIGN KEY (SUPPLIER_ID) REFERENCES SUPPLIER(SUPPLIER_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS PART (
    PART_ID INTEGER PRIMARY KEY,
    CANONICAL_PART_ID TEXT NOT NULL UNIQUE,
    PART_NUMBER TEXT NOT NULL UNIQUE,
    CANONICAL_NAME TEXT NOT NULL,
    REVISION TEXT NOT NULL,
    PART_TYPE_ID INTEGER NOT NULL,
    PART_CATEGORY_ID INTEGER NOT NULL,
    UNIT_ID INTEGER NOT NULL,
    LIFECYCLE_STATUS_ID INTEGER NOT NULL,
    MASS_KG REAL,
    NOTES TEXT,
    FOREIGN KEY (PART_TYPE_ID) REFERENCES PART_TYPE(PART_TYPE_ID),
    FOREIGN KEY (PART_CATEGORY_ID) REFERENCES PART_CATEGORY(PART_CATEGORY_ID),
    FOREIGN KEY (UNIT_ID) REFERENCES UNIT(UNIT_ID),
    FOREIGN KEY (LIFECYCLE_STATUS_ID) REFERENCES LIFECYCLE_STATUS(LIFECYCLE_STATUS_ID)
);

CREATE TABLE IF NOT EXISTS ASSEMBLY (
    ASSEMBLY_ID INTEGER PRIMARY KEY,
    PART_ID INTEGER NOT NULL UNIQUE,
    ASSEMBLY_CODE TEXT NOT NULL UNIQUE,
    MISSION_PHASE TEXT,
    CRITICALITY TEXT,
    PARENT_PROGRAM TEXT,
    FOREIGN KEY (PART_ID) REFERENCES PART(PART_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS SUPPLY (
    SUPPLY_ID INTEGER PRIMARY KEY,
    CANONICAL_SUPPLY_ID TEXT NOT NULL UNIQUE,
    SUPPLY_CODE TEXT NOT NULL UNIQUE,
    CANONICAL_NAME TEXT NOT NULL,
    SUPPLY_CATEGORY_ID INTEGER NOT NULL,
    UNIT_ID INTEGER NOT NULL,
    LIFECYCLE_STATUS_ID INTEGER NOT NULL,
    HAZMAT_FLAG INTEGER NOT NULL DEFAULT 0,
    STORAGE_CLASS TEXT,
    NOTES TEXT,
    FOREIGN KEY (SUPPLY_CATEGORY_ID) REFERENCES SUPPLY_CATEGORY(SUPPLY_CATEGORY_ID),
    FOREIGN KEY (UNIT_ID) REFERENCES UNIT(UNIT_ID),
    FOREIGN KEY (LIFECYCLE_STATUS_ID) REFERENCES LIFECYCLE_STATUS(LIFECYCLE_STATUS_ID)
);

CREATE TABLE IF NOT EXISTS PART_ALIAS (
    PART_ALIAS_ID INTEGER PRIMARY KEY,
    PART_ID INTEGER NOT NULL,
    ALIAS_TEXT TEXT NOT NULL,
    ALIAS_KIND TEXT NOT NULL,
    UNIQUE (PART_ID, ALIAS_TEXT),
    FOREIGN KEY (PART_ID) REFERENCES PART(PART_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS SUPPLY_ALIAS (
    SUPPLY_ALIAS_ID INTEGER PRIMARY KEY,
    SUPPLY_ID INTEGER NOT NULL,
    ALIAS_TEXT TEXT NOT NULL,
    ALIAS_KIND TEXT NOT NULL,
    UNIQUE (SUPPLY_ID, ALIAS_TEXT),
    FOREIGN KEY (SUPPLY_ID) REFERENCES SUPPLY(SUPPLY_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS SUPPLIER_PART (
    SUPPLIER_PART_ID INTEGER PRIMARY KEY,
    SUPPLIER_ID INTEGER NOT NULL,
    PART_ID INTEGER NOT NULL,
    SUPPLIER_PART_NUMBER TEXT NOT NULL,
    APPROVED_STATUS TEXT NOT NULL,
    LEAD_TIME_DAY INTEGER,
    MIN_ORDER_QTY REAL,
    CURRENCY_CODE TEXT NOT NULL,
    BASE_UNIT_PRICE REAL NOT NULL,
    PRICE_BASIS TEXT NOT NULL,
    UNIQUE (SUPPLIER_ID, SUPPLIER_PART_NUMBER),
    FOREIGN KEY (SUPPLIER_ID) REFERENCES SUPPLIER(SUPPLIER_ID) ON DELETE CASCADE,
    FOREIGN KEY (PART_ID) REFERENCES PART(PART_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS SUPPLIER_SUPPLY (
    SUPPLIER_SUPPLY_ID INTEGER PRIMARY KEY,
    SUPPLIER_ID INTEGER NOT NULL,
    SUPPLY_ID INTEGER NOT NULL,
    SUPPLIER_SUPPLY_NUMBER TEXT NOT NULL,
    APPROVED_STATUS TEXT NOT NULL,
    LEAD_TIME_DAY INTEGER,
    MIN_ORDER_QTY REAL,
    CURRENCY_CODE TEXT NOT NULL,
    BASE_UNIT_PRICE REAL NOT NULL,
    PRICE_BASIS TEXT NOT NULL,
    UNIQUE (SUPPLIER_ID, SUPPLIER_SUPPLY_NUMBER),
    FOREIGN KEY (SUPPLIER_ID) REFERENCES SUPPLIER(SUPPLIER_ID) ON DELETE CASCADE,
    FOREIGN KEY (SUPPLY_ID) REFERENCES SUPPLY(SUPPLY_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS PART_ATTRIBUTE (
    PART_ATTRIBUTE_ID INTEGER PRIMARY KEY,
    PART_ID INTEGER NOT NULL,
    ATTRIBUTE_DEFINITION_ID INTEGER NOT NULL,
    ATTRIBUTE_VALUE_TEXT TEXT,
    ATTRIBUTE_VALUE_NUMERIC REAL,
    UNIT_ID INTEGER,
    VALUE_PROVENANCE TEXT NOT NULL DEFAULT 'SEEDED',
    UNIQUE (PART_ID, ATTRIBUTE_DEFINITION_ID),
    FOREIGN KEY (PART_ID) REFERENCES PART(PART_ID) ON DELETE CASCADE,
    FOREIGN KEY (ATTRIBUTE_DEFINITION_ID) REFERENCES ATTRIBUTE_DEFINITION(ATTRIBUTE_DEFINITION_ID),
    FOREIGN KEY (UNIT_ID) REFERENCES UNIT(UNIT_ID)
);

CREATE TABLE IF NOT EXISTS SUPPLY_ATTRIBUTE (
    SUPPLY_ATTRIBUTE_ID INTEGER PRIMARY KEY,
    SUPPLY_ID INTEGER NOT NULL,
    ATTRIBUTE_DEFINITION_ID INTEGER NOT NULL,
    ATTRIBUTE_VALUE_TEXT TEXT,
    ATTRIBUTE_VALUE_NUMERIC REAL,
    UNIT_ID INTEGER,
    VALUE_PROVENANCE TEXT NOT NULL DEFAULT 'SEEDED',
    UNIQUE (SUPPLY_ID, ATTRIBUTE_DEFINITION_ID),
    FOREIGN KEY (SUPPLY_ID) REFERENCES SUPPLY(SUPPLY_ID) ON DELETE CASCADE,
    FOREIGN KEY (ATTRIBUTE_DEFINITION_ID) REFERENCES ATTRIBUTE_DEFINITION(ATTRIBUTE_DEFINITION_ID),
    FOREIGN KEY (UNIT_ID) REFERENCES UNIT(UNIT_ID)
);

CREATE TABLE IF NOT EXISTS ASSEMBLY_BOM_LINE (
    ASSEMBLY_BOM_LINE_ID INTEGER PRIMARY KEY,
    ASSEMBLY_ID INTEGER NOT NULL,
    FIND_NUMBER INTEGER NOT NULL,
    CHILD_PART_ID INTEGER NOT NULL,
    QUANTITY REAL NOT NULL,
    UNIT_ID INTEGER NOT NULL,
    LINE_NOTE TEXT,
    UNIQUE (ASSEMBLY_ID, FIND_NUMBER),
    FOREIGN KEY (ASSEMBLY_ID) REFERENCES ASSEMBLY(ASSEMBLY_ID) ON DELETE CASCADE,
    FOREIGN KEY (CHILD_PART_ID) REFERENCES PART(PART_ID),
    FOREIGN KEY (UNIT_ID) REFERENCES UNIT(UNIT_ID)
);

CREATE TABLE IF NOT EXISTS ASSEMBLY_SUPPLY_REQUIREMENT (
    ASSEMBLY_SUPPLY_REQUIREMENT_ID INTEGER PRIMARY KEY,
    ASSEMBLY_ID INTEGER NOT NULL,
    SUPPLY_ID INTEGER NOT NULL,
    QUANTITY REAL NOT NULL,
    UNIT_ID INTEGER NOT NULL,
    USE_CONTEXT TEXT,
    UNIQUE (ASSEMBLY_ID, SUPPLY_ID, USE_CONTEXT),
    FOREIGN KEY (ASSEMBLY_ID) REFERENCES ASSEMBLY(ASSEMBLY_ID) ON DELETE CASCADE,
    FOREIGN KEY (SUPPLY_ID) REFERENCES SUPPLY(SUPPLY_ID),
    FOREIGN KEY (UNIT_ID) REFERENCES UNIT(UNIT_ID)
);

CREATE TABLE IF NOT EXISTS GENERATED_RUN (
    GENERATED_RUN_ID INTEGER PRIMARY KEY,
    RUN_LABEL TEXT NOT NULL UNIQUE,
    ROOT_DIR TEXT NOT NULL,
    CONFIG_JSON TEXT NOT NULL,
    CREATED_AT TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS GENERATED_DOCUMENT (
    GENERATED_DOCUMENT_ID INTEGER PRIMARY KEY,
    GENERATED_RUN_ID INTEGER NOT NULL,
    PACKAGE_ID TEXT NOT NULL,
    DOCUMENT_TYPE_ID INTEGER NOT NULL,
    DOC_ID TEXT NOT NULL,
    GENERATION_SOURCE TEXT NOT NULL,
    MODEL_NAME TEXT,
    ASSEMBLY_ID INTEGER NOT NULL,
    FILE_PATH TEXT NOT NULL,
    CONTENT_TEXT TEXT NOT NULL,
    CREATED_AT TEXT NOT NULL,
    UNIQUE (GENERATED_RUN_ID, DOC_ID),
    FOREIGN KEY (GENERATED_RUN_ID) REFERENCES GENERATED_RUN(GENERATED_RUN_ID) ON DELETE CASCADE,
    FOREIGN KEY (DOCUMENT_TYPE_ID) REFERENCES DOCUMENT_TYPE(DOCUMENT_TYPE_ID),
    FOREIGN KEY (ASSEMBLY_ID) REFERENCES ASSEMBLY(ASSEMBLY_ID)
);

CREATE TABLE IF NOT EXISTS GENERATED_DOCUMENT_ENTITY (
    GENERATED_DOCUMENT_ENTITY_ID INTEGER PRIMARY KEY,
    GENERATED_DOCUMENT_ID INTEGER NOT NULL,
    ENTITY_KIND TEXT NOT NULL CHECK (ENTITY_KIND IN ('PART','SUPPLY','SUPPLIER','ASSEMBLY')),
    PART_ID INTEGER,
    SUPPLY_ID INTEGER,
    SUPPLIER_ID INTEGER,
    ASSEMBLY_ID INTEGER,
    ROLE_CODE TEXT NOT NULL,
    SURFACE_FORM TEXT NOT NULL,
    EVIDENCE_SOURCE TEXT NOT NULL,
    FOREIGN KEY (GENERATED_DOCUMENT_ID) REFERENCES GENERATED_DOCUMENT(GENERATED_DOCUMENT_ID) ON DELETE CASCADE,
    FOREIGN KEY (PART_ID) REFERENCES PART(PART_ID),
    FOREIGN KEY (SUPPLY_ID) REFERENCES SUPPLY(SUPPLY_ID),
    FOREIGN KEY (SUPPLIER_ID) REFERENCES SUPPLIER(SUPPLIER_ID),
    FOREIGN KEY (ASSEMBLY_ID) REFERENCES ASSEMBLY(ASSEMBLY_ID)
);

CREATE TABLE IF NOT EXISTS NER_RUN (
    NER_RUN_ID INTEGER PRIMARY KEY,
    RUN_LABEL TEXT NOT NULL UNIQUE,
    GENERATED_RUN_ID INTEGER NOT NULL,
    CONFIG_JSON TEXT NOT NULL,
    CREATED_AT TEXT NOT NULL,
    FOREIGN KEY (GENERATED_RUN_ID) REFERENCES GENERATED_RUN(GENERATED_RUN_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS NER_MENTION (
    NER_MENTION_ID INTEGER PRIMARY KEY,
    NER_RUN_ID INTEGER NOT NULL,
    GENERATED_DOCUMENT_ID INTEGER NOT NULL,
    MENTION_TEXT TEXT NOT NULL,
    START_OFFSET INTEGER,
    END_OFFSET INTEGER,
    ENTITY_KIND_GUESS TEXT NOT NULL,
    EXTRACTION_METHOD TEXT NOT NULL,
    CONTEXT_SNIPPET TEXT,
    FOREIGN KEY (NER_RUN_ID) REFERENCES NER_RUN(NER_RUN_ID) ON DELETE CASCADE,
    FOREIGN KEY (GENERATED_DOCUMENT_ID) REFERENCES GENERATED_DOCUMENT(GENERATED_DOCUMENT_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS NER_RESOLUTION (
    NER_RESOLUTION_ID INTEGER PRIMARY KEY,
    NER_MENTION_ID INTEGER NOT NULL UNIQUE,
    RESOLVED_ENTITY_KIND TEXT NOT NULL,
    PART_ID INTEGER,
    SUPPLY_ID INTEGER,
    SUPPLIER_ID INTEGER,
    ASSEMBLY_ID INTEGER,
    RESOLUTION_METHOD TEXT NOT NULL,
    CONFIDENCE_SCORE REAL NOT NULL,
    PROVENANCE_STRENGTH TEXT NOT NULL,
    EVIDENCE_JSON TEXT NOT NULL,
    FOREIGN KEY (NER_MENTION_ID) REFERENCES NER_MENTION(NER_MENTION_ID) ON DELETE CASCADE,
    FOREIGN KEY (PART_ID) REFERENCES PART(PART_ID),
    FOREIGN KEY (SUPPLY_ID) REFERENCES SUPPLY(SUPPLY_ID),
    FOREIGN KEY (SUPPLIER_ID) REFERENCES SUPPLIER(SUPPLIER_ID),
    FOREIGN KEY (ASSEMBLY_ID) REFERENCES ASSEMBLY(ASSEMBLY_ID)
);

CREATE TABLE IF NOT EXISTS NER_CONFLICT (
    NER_CONFLICT_ID INTEGER PRIMARY KEY,
    NER_RUN_ID INTEGER NOT NULL,
    GENERATED_DOCUMENT_ID INTEGER,
    CONFLICT_TYPE TEXT NOT NULL,
    SEVERITY TEXT NOT NULL,
    DESCRIPTION_TEXT TEXT NOT NULL,
    RELATED_MENTION_ID INTEGER,
    RELATED_PART_ID INTEGER,
    RELATED_SUPPLY_ID INTEGER,
    CREATED_AT TEXT NOT NULL,
    FOREIGN KEY (NER_RUN_ID) REFERENCES NER_RUN(NER_RUN_ID) ON DELETE CASCADE,
    FOREIGN KEY (GENERATED_DOCUMENT_ID) REFERENCES GENERATED_DOCUMENT(GENERATED_DOCUMENT_ID),
    FOREIGN KEY (RELATED_MENTION_ID) REFERENCES NER_MENTION(NER_MENTION_ID),
    FOREIGN KEY (RELATED_PART_ID) REFERENCES PART(PART_ID),
    FOREIGN KEY (RELATED_SUPPLY_ID) REFERENCES SUPPLY(SUPPLY_ID)
);

CREATE TABLE IF NOT EXISTS NER_PACKAGE_COVERAGE (
    NER_PACKAGE_COVERAGE_ID INTEGER PRIMARY KEY,
    NER_RUN_ID INTEGER NOT NULL,
    PACKAGE_ID TEXT NOT NULL,
    ASSEMBLY_ID INTEGER NOT NULL,
    COMPONENT_MATCH_RATE REAL NOT NULL,
    SUPPLY_MATCH_RATE REAL NOT NULL,
    COMPONENT_EXPECTED_COUNT INTEGER NOT NULL,
    COMPONENT_FOUND_COUNT INTEGER NOT NULL,
    SUPPLY_EXPECTED_COUNT INTEGER NOT NULL,
    SUPPLY_FOUND_COUNT INTEGER NOT NULL,
    FOREIGN KEY (NER_RUN_ID) REFERENCES NER_RUN(NER_RUN_ID) ON DELETE CASCADE,
    FOREIGN KEY (ASSEMBLY_ID) REFERENCES ASSEMBLY(ASSEMBLY_ID)
);

CREATE TABLE IF NOT EXISTS NER_RECONSTRUCTED_BOM_LINE (
    NER_RECONSTRUCTED_BOM_LINE_ID INTEGER PRIMARY KEY,
    NER_RUN_ID INTEGER NOT NULL,
    PACKAGE_ID TEXT NOT NULL,
    ASSEMBLY_ID INTEGER NOT NULL,
    PART_ID INTEGER NOT NULL,
    MENTION_COUNT INTEGER NOT NULL,
    FOREIGN KEY (NER_RUN_ID) REFERENCES NER_RUN(NER_RUN_ID) ON DELETE CASCADE,
    FOREIGN KEY (ASSEMBLY_ID) REFERENCES ASSEMBLY(ASSEMBLY_ID),
    FOREIGN KEY (PART_ID) REFERENCES PART(PART_ID)
);

CREATE TABLE IF NOT EXISTS NER_RECONSTRUCTED_SUPPLY_REQUIREMENT (
    NER_RECONSTRUCTED_SUPPLY_REQUIREMENT_ID INTEGER PRIMARY KEY,
    NER_RUN_ID INTEGER NOT NULL,
    PACKAGE_ID TEXT NOT NULL,
    ASSEMBLY_ID INTEGER NOT NULL,
    SUPPLY_ID INTEGER NOT NULL,
    MENTION_COUNT INTEGER NOT NULL,
    FOREIGN KEY (NER_RUN_ID) REFERENCES NER_RUN(NER_RUN_ID) ON DELETE CASCADE,
    FOREIGN KEY (ASSEMBLY_ID) REFERENCES ASSEMBLY(ASSEMBLY_ID),
    FOREIGN KEY (SUPPLY_ID) REFERENCES SUPPLY(SUPPLY_ID)
);

CREATE TABLE IF NOT EXISTS AUDIT_RUN (
    AUDIT_RUN_ID INTEGER PRIMARY KEY,
    RUN_LABEL TEXT NOT NULL UNIQUE,
    NER_RUN_ID INTEGER NOT NULL,
    MODEL_NAME TEXT NOT NULL,
    CREATED_AT TEXT NOT NULL,
    CONFIG_JSON TEXT NOT NULL,
    FOREIGN KEY (NER_RUN_ID) REFERENCES NER_RUN(NER_RUN_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS AUDIT_CASE (
    AUDIT_CASE_ID INTEGER PRIMARY KEY,
    AUDIT_RUN_ID INTEGER NOT NULL,
    CASE_TYPE TEXT NOT NULL,
    SEVERITY TEXT NOT NULL,
    STATUS_CODE TEXT NOT NULL,
    GENERATED_DOCUMENT_ID INTEGER,
    PART_ID INTEGER,
    SUPPLY_ID INTEGER,
    ASSEMBLY_ID INTEGER,
    SOURCE_RESOLUTION_ID INTEGER,
    PROMPT_TEXT TEXT NOT NULL,
    RESPONSE_TEXT TEXT,
    CREATED_AT TEXT NOT NULL,
    FOREIGN KEY (AUDIT_RUN_ID) REFERENCES AUDIT_RUN(AUDIT_RUN_ID) ON DELETE CASCADE,
    FOREIGN KEY (GENERATED_DOCUMENT_ID) REFERENCES GENERATED_DOCUMENT(GENERATED_DOCUMENT_ID),
    FOREIGN KEY (PART_ID) REFERENCES PART(PART_ID),
    FOREIGN KEY (SUPPLY_ID) REFERENCES SUPPLY(SUPPLY_ID),
    FOREIGN KEY (ASSEMBLY_ID) REFERENCES ASSEMBLY(ASSEMBLY_ID),
    FOREIGN KEY (SOURCE_RESOLUTION_ID) REFERENCES NER_RESOLUTION(NER_RESOLUTION_ID)
);

CREATE TABLE IF NOT EXISTS AUDIT_FINDING (
    AUDIT_FINDING_ID INTEGER PRIMARY KEY,
    AUDIT_CASE_ID INTEGER NOT NULL,
    FINDING_CODE TEXT NOT NULL,
    RISK_LEVEL TEXT NOT NULL,
    SUMMARY_TEXT TEXT NOT NULL,
    RECOMMENDATION_TEXT TEXT,
    CREATED_AT TEXT NOT NULL,
    FOREIGN KEY (AUDIT_CASE_ID) REFERENCES AUDIT_CASE(AUDIT_CASE_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS LLM_CALL (
    LLM_CALL_ID INTEGER PRIMARY KEY,
    STAGE_CODE TEXT NOT NULL,
    ROLE_CODE TEXT NOT NULL,
    MODEL_NAME TEXT,
    RUN_CONTEXT TEXT,
    GENERATED_DOCUMENT_ID INTEGER,
    NER_MENTION_ID INTEGER,
    AUDIT_CASE_ID INTEGER,
    PROMPT_TEXT TEXT,
    RESPONSE_TEXT TEXT,
    SUCCESS_FLAG INTEGER NOT NULL DEFAULT 1,
    CREATED_AT TEXT NOT NULL,
    FOREIGN KEY (GENERATED_DOCUMENT_ID) REFERENCES GENERATED_DOCUMENT(GENERATED_DOCUMENT_ID) ON DELETE SET NULL,
    FOREIGN KEY (NER_MENTION_ID) REFERENCES NER_MENTION(NER_MENTION_ID) ON DELETE SET NULL,
    FOREIGN KEY (AUDIT_CASE_ID) REFERENCES AUDIT_CASE(AUDIT_CASE_ID) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS DQ_RUN (
    DQ_RUN_ID INTEGER PRIMARY KEY,
    RUN_LABEL TEXT NOT NULL UNIQUE,
    NER_RUN_ID INTEGER NOT NULL,
    AUDIT_RUN_ID INTEGER,
    REPORT_HTML_PATH TEXT NOT NULL,
    CREATED_AT TEXT NOT NULL,
    FOREIGN KEY (NER_RUN_ID) REFERENCES NER_RUN(NER_RUN_ID) ON DELETE CASCADE,
    FOREIGN KEY (AUDIT_RUN_ID) REFERENCES AUDIT_RUN(AUDIT_RUN_ID)
);

CREATE TABLE IF NOT EXISTS DQ_METRIC (
    DQ_METRIC_ID INTEGER PRIMARY KEY,
    DQ_RUN_ID INTEGER NOT NULL,
    METRIC_CODE TEXT NOT NULL,
    METRIC_VALUE REAL NOT NULL,
    METRIC_UNIT TEXT,
    METRIC_CONTEXT TEXT,
    FOREIGN KEY (DQ_RUN_ID) REFERENCES DQ_RUN(DQ_RUN_ID) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS UX_DQ_METRIC_RUN_CODE_CONTEXT
ON DQ_METRIC (DQ_RUN_ID, METRIC_CODE, METRIC_CONTEXT);

CREATE TABLE IF NOT EXISTS DQ_FINDING (
    DQ_FINDING_ID INTEGER PRIMARY KEY,
    DQ_RUN_ID INTEGER NOT NULL,
    FINDING_TYPE TEXT NOT NULL,
    SEVERITY TEXT NOT NULL,
    SUMMARY_TEXT TEXT NOT NULL,
    DETAIL_TEXT TEXT,
    FOREIGN KEY (DQ_RUN_ID) REFERENCES DQ_RUN(DQ_RUN_ID) ON DELETE CASCADE
);
"""


LOOKUP_DATA = {
    "UNIT": [
        ("EA", "Each"), ("KIT", "Kit"), ("L", "Liter"), ("G", "Gram"), ("ML", "Milliliter"),
        ("ROLL", "Roll"), ("BOX", "Box"), ("M", "Meter"), ("BAR", "Bar"), ("W", "Watt"), ("V", "Volt"),
        ("C", "Celsius"),
    ],
    "LIFECYCLE_STATUS": [("ACTIVE", "Active"), ("QUALIFIED", "Qualified"), ("LEGACY", "Legacy"), ("EXPERIMENTAL", "Experimental")],
    "PART_TYPE": [("COMPONENT", "Component"), ("ASSEMBLY", "Assembly")],
    "PART_CATEGORY": [
        ("CRYO_VALVE", "Cryogenic Valve"), ("SENSOR", "Sensor"), ("POWER", "Power"), ("STRUCTURE", "Structure"),
        ("COMPUTE", "Compute"), ("THERMAL", "Thermal"), ("FLOW", "Flow"), ("RADIATION", "Radiation"),
        ("SAMPLE", "Sample Handling"), ("ASSY", "Assembly"),
    ],
    "SUPPLY_CATEGORY": [
        ("ADHESIVE", "Adhesive"), ("LUBRICANT", "Lubricant"), ("STERILE", "Sterile Consumable"),
        ("PACKAGING", "Packaging"), ("CLEANING", "Cleaning"), ("FASTENER_KIT", "Fastener Kit"),
    ],
    "DOCUMENT_TYPE": [
        ("ENGINEERING_SPEC", "Engineering Specification"), ("SUPPLIER_QUOTE", "Supplier Quote"),
        ("EMAIL", "Email Thread"), ("MAINTENANCE_NOTE", "Maintenance Note"), ("TEST_REPORT", "Test Report"),
        ("PRICE_LIST", "Price List"),
    ],
}


def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(load_schema_sql())
    conn.commit()


def fetch_lookup_id(conn: sqlite3.Connection, table: str, code_col: str, code: str) -> int:
    row = conn.execute(f"SELECT {table}_ID AS ID FROM {table} WHERE {code_col} = ?", (code,)).fetchone()
    assert row, (table, code)
    return int(row["ID"])


def seed_lookups(conn: sqlite3.Connection) -> None:
    for table, rows in LOOKUP_DATA.items():
        if conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]:
            continue
        if table == "UNIT":
            conn.executemany("INSERT INTO UNIT (UNIT_CODE, UNIT_NAME) VALUES (?, ?)", rows)
        elif table == "LIFECYCLE_STATUS":
            conn.executemany("INSERT INTO LIFECYCLE_STATUS (STATUS_CODE, STATUS_NAME) VALUES (?, ?)", rows)
        elif table == "PART_TYPE":
            conn.executemany("INSERT INTO PART_TYPE (PART_TYPE_CODE, PART_TYPE_NAME) VALUES (?, ?)", rows)
        elif table == "PART_CATEGORY":
            conn.executemany("INSERT INTO PART_CATEGORY (CATEGORY_CODE, CATEGORY_NAME) VALUES (?, ?)", rows)
        elif table == "SUPPLY_CATEGORY":
            conn.executemany("INSERT INTO SUPPLY_CATEGORY (CATEGORY_CODE, CATEGORY_NAME) VALUES (?, ?)", rows)
        elif table == "DOCUMENT_TYPE":
            conn.executemany("INSERT INTO DOCUMENT_TYPE (TYPE_CODE, TYPE_NAME) VALUES (?, ?)", rows)
    conn.commit()


def seed_attribute_definitions(conn: sqlite3.Connection) -> None:
    if conn.execute("SELECT COUNT(*) FROM ATTRIBUTE_DEFINITION").fetchone()[0]:
        return
    unit_ids = {row["UNIT_CODE"]: row["UNIT_ID"] for row in conn.execute("SELECT UNIT_ID, UNIT_CODE FROM UNIT")}
    rows = [
        ("PART", "MATERIAL", "Material", "TEXT", None),
        ("PART", "CRITICALITY", "Criticality", "TEXT", None),
        ("PART", "OPERATING_TEMP_MIN_C", "Operating Temp Min C", "NUMERIC", unit_ids["C"]),
        ("PART", "OPERATING_TEMP_MAX_C", "Operating Temp Max C", "NUMERIC", unit_ids["C"]),
        ("PART", "DIMENSIONS_TEXT", "Dimensions", "TEXT", None),
        ("PART", "VOLTAGE_V", "Voltage", "NUMERIC", unit_ids["V"]),
        ("PART", "POWER_W", "Power", "NUMERIC", unit_ids["W"]),
        ("PART", "PRESSURE_RATING_BAR", "Pressure Rating", "NUMERIC", unit_ids["BAR"]),
        ("PART", "CONFIGURATION_STATUS", "Configuration Status", "TEXT", None),
        ("SUPPLY", "MATERIAL_FAMILY", "Material Family", "TEXT", None),
        ("SUPPLY", "SHELF_LIFE_DAY", "Shelf Life Day", "NUMERIC", None),
        ("SUPPLY", "STERILE_FLAG", "Sterile Flag", "TEXT", None),
        ("SUPPLY", "CONTAINER_TYPE", "Container Type", "TEXT", None),
    ]
    conn.executemany(
        "INSERT INTO ATTRIBUTE_DEFINITION (ENTITY_KIND, ATTRIBUTE_CODE, ATTRIBUTE_NAME, VALUE_TYPE, DEFAULT_UNIT_ID) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()

# ---------------------------
# Seed synthetic data
# ---------------------------

COMPONENT_BASES = [
    ("Cryogenic Valve Actuator", "CRYO_VALVE", "titanium", 1.2),
    ("Europa Sample Transfer Pump", "FLOW", "stainless", 2.6),
    ("Ice Penetration Thermal Probe", "THERMAL", "nickel alloy", 3.1),
    ("Radiation-Hardened Control Board", "COMPUTE", "ceramic composite", 0.9),
    ("Brine Spectral Sensor", "SENSOR", "quartz", 0.4),
    ("Microscope Positioning Rail", "STRUCTURE", "aluminum", 1.8),
    ("Sterile Sample Gate", "SAMPLE", "ptfe coated steel", 0.8),
    ("Pressure Equalization Manifold", "FLOW", "inconel", 1.6),
    ("Cryo Seal Ring", "CRYO_VALVE", "ptfe", 0.1),
    ("Battery Isolation Module", "POWER", "composite", 1.5),
]

ASSEMBLY_BASES = [
    ("Cryogenic Sampling Assembly", "ASSY", "surface sampling", "critical"),
    ("Subsurface Fluid Handling Assembly", "ASSY", "subsurface drilling", "high"),
    ("Sterile Intake Assembly", "ASSY", "sample protection", "critical"),
    ("Thermal Management Assembly", "ASSY", "survival heating", "high"),
    ("Radiation Shield Assembly", "ASSY", "payload protection", "high"),
    ("Life Detection Cartridge Assembly", "ASSY", "instrument operation", "critical"),
]

SUPPLY_BASES = [
    ("Cryogenic Thermal Grease", "ADHESIVE", "L"),
    ("Sterile Wipe Kit", "STERILE", "KIT"),
    ("Nitrile Glove Pack", "STERILE", "BOX"),
    ("Sample Sealant Gel", "ADHESIVE", "ML"),
    ("Instrument Cleaning Solvent", "CLEANING", "L"),
    ("Vacuum Bag Roll", "PACKAGING", "ROLL"),
    ("Fastener Service Kit", "FASTENER_KIT", "KIT"),
    ("Bearing Micro-Lubricant", "LUBRICANT", "ML"),
    ("Sterile Swab Set", "STERILE", "KIT"),
]

SUPPLIERS = [
    ("SUP-001", "Helios Dynamics", "US", "T1"),
    ("SUP-002", "Aster Forge", "CA", "T1"),
    ("SUP-003", "Blue Arc Cryo", "DE", "T2"),
    ("SUP-004", "Europa Field Systems", "NO", "T2"),
    ("SUP-005", "Polar Sterile Works", "SE", "T2"),
    ("SUP-006", "Vector Sample Logistics", "NL", "T3"),
    ("SUP-007", "Orbit Materials Lab", "US", "T2"),
    ("SUP-008", "Deep Cold Process", "GB", "T3"),
]


def seed_master_data(conn: sqlite3.Connection, cfg: dict[str, Any]) -> dict[str, int]:
    if conn.execute("SELECT COUNT(*) FROM PART").fetchone()[0] > 0:
        return {
            "suppliers": conn.execute("SELECT COUNT(*) FROM SUPPLIER").fetchone()[0],
            "components": conn.execute("SELECT COUNT(*) FROM PART p JOIN PART_TYPE pt ON pt.PART_TYPE_ID=p.PART_TYPE_ID WHERE pt.PART_TYPE_CODE='COMPONENT'").fetchone()[0],
            "assemblies": conn.execute("SELECT COUNT(*) FROM ASSEMBLY").fetchone()[0],
            "supplies": conn.execute("SELECT COUNT(*) FROM SUPPLY").fetchone()[0],
            "bom_lines": conn.execute("SELECT COUNT(*) FROM ASSEMBLY_BOM_LINE").fetchone()[0],
            "assembly_supply_requirements": conn.execute("SELECT COUNT(*) FROM ASSEMBLY_SUPPLY_REQUIREMENT").fetchone()[0],
        }

    rng = random.Random(int(cfg.get("seed", 42)))
    seed_lookups(conn)
    seed_attribute_definitions(conn)

    # Suppliers
    conn.executemany(
        "INSERT INTO SUPPLIER (SUPPLIER_CODE, CANONICAL_NAME, COUNTRY_CODE, SUPPLIER_TIER) VALUES (?, ?, ?, ?)",
        SUPPLIERS,
    )
    supplier_rows = {row["SUPPLIER_CODE"]: dict(row) for row in conn.execute("SELECT * FROM SUPPLIER")}
    for code, name, *_ in SUPPLIERS:
        aliases = [
            (supplier_rows[code]["SUPPLIER_ID"], name.replace("Dynamics", "Dyn."), "SHORT_NAME"),
            (supplier_rows[code]["SUPPLIER_ID"], name.upper(), "UPPER_CASE"),
        ]
        conn.executemany("INSERT INTO SUPPLIER_ALIAS (SUPPLIER_ID, ALIAS_TEXT, ALIAS_KIND) VALUES (?, ?, ?)", aliases)

    unit_ids = {row["UNIT_CODE"]: row["UNIT_ID"] for row in conn.execute("SELECT UNIT_ID, UNIT_CODE FROM UNIT")}
    lifecycle_ids = {row["STATUS_CODE"]: row["LIFECYCLE_STATUS_ID"] for row in conn.execute("SELECT * FROM LIFECYCLE_STATUS")}
    part_type_ids = {row["PART_TYPE_CODE"]: row["PART_TYPE_ID"] for row in conn.execute("SELECT * FROM PART_TYPE")}
    part_cat_ids = {row["CATEGORY_CODE"]: row["PART_CATEGORY_ID"] for row in conn.execute("SELECT * FROM PART_CATEGORY")}
    supply_cat_ids = {row["CATEGORY_CODE"]: row["SUPPLY_CATEGORY_ID"] for row in conn.execute("SELECT * FROM SUPPLY_CATEGORY")}

    component_count = int(cfg.get("component_count", 120))
    assembly_count = int(cfg.get("assembly_count", 18))
    supply_count = int(cfg.get("supply_count", 36))

    # Parts / components
    for i in range(1, component_count + 1):
        base_name, cat, material, mass = COMPONENT_BASES[(i - 1) % len(COMPONENT_BASES)]
        name = f"{base_name} {chr(64 + ((i - 1) % 26) + 1)}{(i - 1) // 26 + 1}"
        part_number = title_case_code("CMP", i, 4)
        lifecycle = rng.choices(["ACTIVE", "QUALIFIED", "LEGACY"], weights=[70, 20, 10], k=1)[0]
        rev = rng.choice(list("ABCD"))
        conn.execute(
            "INSERT INTO PART (CANONICAL_PART_ID, PART_NUMBER, CANONICAL_NAME, REVISION, PART_TYPE_ID, PART_CATEGORY_ID, UNIT_ID, LIFECYCLE_STATUS_ID, MASS_KG, NOTES) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"CMP-{i:04d}",
                part_number,
                name,
                rev,
                part_type_ids["COMPONENT"],
                part_cat_ids[cat],
                unit_ids["EA"],
                lifecycle_ids[lifecycle],
                round(mass * rng.uniform(0.75, 1.25), 3),
                f"Seeded synthetic component {i}",
            ),
        )
    # Assemblies are also PART rows
    for i in range(1, assembly_count + 1):
        base_name, cat, phase, criticality = ASSEMBLY_BASES[(i - 1) % len(ASSEMBLY_BASES)]
        part_number = title_case_code("ASM", i, 3)
        conn.execute(
            "INSERT INTO PART (CANONICAL_PART_ID, PART_NUMBER, CANONICAL_NAME, REVISION, PART_TYPE_ID, PART_CATEGORY_ID, UNIT_ID, LIFECYCLE_STATUS_ID, MASS_KG, NOTES) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"ASM-{i:03d}",
                part_number,
                f"{base_name} {i:02d}",
                rng.choice(["A", "B", "C"]),
                part_type_ids["ASSEMBLY"],
                part_cat_ids[cat],
                unit_ids["EA"],
                lifecycle_ids["ACTIVE"],
                round(rng.uniform(5, 18), 3),
                f"Assembly for {phase}",
            ),
        )
    part_rows = [dict(r) for r in conn.execute("SELECT * FROM PART ORDER BY PART_ID")]
    component_rows = [r for r in part_rows if r["CANONICAL_PART_ID"].startswith("CMP-")]
    assembly_part_rows = [r for r in part_rows if r["CANONICAL_PART_ID"].startswith("ASM-")]

    # Assembly
    for i, part in enumerate(assembly_part_rows, start=1):
        base_name, _cat, phase, criticality = ASSEMBLY_BASES[(i - 1) % len(ASSEMBLY_BASES)]
        conn.execute(
            "INSERT INTO ASSEMBLY (PART_ID, ASSEMBLY_CODE, MISSION_PHASE, CRITICALITY, PARENT_PROGRAM) VALUES (?, ?, ?, ?, ?)",
            (part["PART_ID"], part["PART_NUMBER"], phase, criticality.upper(), "EUROPA_LIFE"),
        )

    # Supplies
    for i in range(1, supply_count + 1):
        base_name, cat, unit = SUPPLY_BASES[(i - 1) % len(SUPPLY_BASES)]
        conn.execute(
            "INSERT INTO SUPPLY (CANONICAL_SUPPLY_ID, SUPPLY_CODE, CANONICAL_NAME, SUPPLY_CATEGORY_ID, UNIT_ID, LIFECYCLE_STATUS_ID, HAZMAT_FLAG, STORAGE_CLASS, NOTES) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"SUPPLY-{i:03d}",
                title_case_code("SUPPLY", i, 3),
                f"{base_name} {i:02d}",
                supply_cat_ids[cat],
                unit_ids[unit],
                lifecycle_ids[rng.choice(["ACTIVE", "QUALIFIED"])],
                1 if cat in {"ADHESIVE", "CLEANING"} and rng.random() < 0.4 else 0,
                rng.choice(["COLD", "STERILE", "DRY", "GENERAL"]),
                f"Seeded synthetic supply {i}",
            ),
        )
    supply_rows = [dict(r) for r in conn.execute("SELECT * FROM SUPPLY ORDER BY SUPPLY_ID")]

    # Aliases
    for p in component_rows + assembly_part_rows:
        aliases = [
            (p["PART_ID"], p["CANONICAL_NAME"].replace("Assembly", "Assy").replace("Cryogenic", "Cryo").replace("Radiation-Hardened", "Rad-Hard"), "SHORT_NAME"),
            (p["PART_ID"], p["PART_NUMBER"].replace("CMP", "C").replace("ASM", "A"), "INTERNAL_CODE"),
            (p["PART_ID"], re.sub(r"\bAssembly\b", "module", p["CANONICAL_NAME"], flags=re.I), "NAME_VARIANT"),
        ]
        for row in aliases:
            conn.execute("INSERT OR IGNORE INTO PART_ALIAS (PART_ID, ALIAS_TEXT, ALIAS_KIND) VALUES (?, ?, ?)", row)
    for s in supply_rows:
        aliases = [
            (s["SUPPLY_ID"], s["CANONICAL_NAME"].replace("Kit", "pkg").replace("Pack", "pk"), "SHORT_NAME"),
            (s["SUPPLY_ID"], s["SUPPLY_CODE"].replace("SUPPLY", "S"), "INTERNAL_CODE"),
        ]
        for row in aliases:
            conn.execute("INSERT OR IGNORE INTO SUPPLY_ALIAS (SUPPLY_ID, ALIAS_TEXT, ALIAS_KIND) VALUES (?, ?, ?)", row)

    # Supplier part and supply prices
    supplier_ids = [r["SUPPLIER_ID"] for r in conn.execute("SELECT SUPPLIER_ID FROM SUPPLIER ORDER BY SUPPLIER_ID")]
    for p in component_rows:
        chosen = rng.sample(supplier_ids, k=rng.randint(2, 3))
        for sid in chosen:
            price = round((p["MASS_KG"] or 1.0) * rng.uniform(1200, 4800), 2)
            conn.execute(
                "INSERT INTO SUPPLIER_PART (SUPPLIER_ID, PART_ID, SUPPLIER_PART_NUMBER, APPROVED_STATUS, LEAD_TIME_DAY, MIN_ORDER_QTY, CURRENCY_CODE, BASE_UNIT_PRICE, PRICE_BASIS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (sid, p["PART_ID"], f"V-{sid}-{p['PART_NUMBER']}", rng.choice(["APPROVED", "PREFERRED", "LEGACY"]), rng.randint(10, 90), rng.choice([1,1,2,5]), "USD", price, "UNIT"),
            )
    for s in supply_rows:
        chosen = rng.sample(supplier_ids, k=rng.randint(1, 3))
        for sid in chosen:
            conn.execute(
                "INSERT INTO SUPPLIER_SUPPLY (SUPPLIER_ID, SUPPLY_ID, SUPPLIER_SUPPLY_NUMBER, APPROVED_STATUS, LEAD_TIME_DAY, MIN_ORDER_QTY, CURRENCY_CODE, BASE_UNIT_PRICE, PRICE_BASIS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (sid, s["SUPPLY_ID"], f"S-{sid}-{s['SUPPLY_CODE']}", rng.choice(["APPROVED", "PREFERRED"]), rng.randint(2, 20), rng.choice([1, 2, 5, 10]), "USD", round(rng.uniform(8, 220), 2), "UNIT"),
            )

    # Attributes
    attr_ids = {(r["ENTITY_KIND"], r["ATTRIBUTE_CODE"]): r["ATTRIBUTE_DEFINITION_ID"] for r in conn.execute("SELECT * FROM ATTRIBUTE_DEFINITION")}
    for p in component_rows + assembly_part_rows:
        values: list[tuple[int, int, str | None, float | None, int | None]] = []
        material = rng.choice(["Titanium", "Aluminum", "Inconel", "Ceramic Composite", "PTFE", "Quartz"])
        criticality = rng.choice(["HIGH", "MEDIUM", "CRITICAL"])
        min_t = rng.randint(-180, -20)
        max_t = rng.randint(40, 180)
        voltage = rng.choice([3.3, 5.0, 12.0, 24.0]) if p["CANONICAL_PART_ID"].startswith("CMP-") else None
        power = round(rng.uniform(1.0, 80.0), 1) if voltage else None
        pressure = round(rng.uniform(1.0, 120.0), 1) if rng.random() < 0.6 else None
        dims = f"{rng.randint(10,120)}x{rng.randint(10,120)}x{rng.randint(5,80)} mm"
        config_status = rng.choice(["BASELINE", "QUALIFIED", "DEV_BUILD"])
        items = [
            (("PART", "MATERIAL"), material, None, None),
            (("PART", "CRITICALITY"), criticality, None, None),
            (("PART", "OPERATING_TEMP_MIN_C"), None, min_t, unit_ids["C"]),
            (("PART", "OPERATING_TEMP_MAX_C"), None, max_t, unit_ids["C"]),
            (("PART", "DIMENSIONS_TEXT"), dims, None, None),
            (("PART", "CONFIGURATION_STATUS"), config_status, None, None),
        ]
        if voltage is not None:
            items.append((("PART", "VOLTAGE_V"), None, voltage, unit_ids["V"]))
        if power is not None:
            items.append((("PART", "POWER_W"), None, power, unit_ids["W"]))
        if pressure is not None:
            items.append((("PART", "PRESSURE_RATING_BAR"), None, pressure, unit_ids["BAR"]))
        for key, text_val, num_val, unit_id in items:
            conn.execute(
                "INSERT INTO PART_ATTRIBUTE (PART_ID, ATTRIBUTE_DEFINITION_ID, ATTRIBUTE_VALUE_TEXT, ATTRIBUTE_VALUE_NUMERIC, UNIT_ID) VALUES (?, ?, ?, ?, ?)",
                (p["PART_ID"], attr_ids[key], text_val, num_val, unit_id),
            )
    for s in supply_rows:
        items = [
            (("SUPPLY", "MATERIAL_FAMILY"), rng.choice(["Polymer", "Alcohol", "Silicone", "Nitrile", "Paper", "Composite"]), None, None),
            (("SUPPLY", "SHELF_LIFE_DAY"), None, rng.choice([90, 180, 365, 730]), None),
            (("SUPPLY", "STERILE_FLAG"), rng.choice(["Y", "N"]), None, None),
            (("SUPPLY", "CONTAINER_TYPE"), rng.choice(["Tube", "Bottle", "Box", "Pouch", "Case"]), None, None),
        ]
        for key, text_val, num_val, unit_id in items:
            conn.execute(
                "INSERT INTO SUPPLY_ATTRIBUTE (SUPPLY_ID, ATTRIBUTE_DEFINITION_ID, ATTRIBUTE_VALUE_TEXT, ATTRIBUTE_VALUE_NUMERIC, UNIT_ID) VALUES (?, ?, ?, ?, ?)",
                (s["SUPPLY_ID"], attr_ids[key], text_val, num_val, unit_id),
            )

    # BOM and supply requirements
    assembly_rows = [dict(r) for r in conn.execute("SELECT * FROM ASSEMBLY ORDER BY ASSEMBLY_ID")]
    for idx, assembly in enumerate(assembly_rows, start=1):
        line_count = rng.randint(8, 12)
        chosen_components = rng.sample(component_rows, k=line_count)
        for find_number, child in enumerate(chosen_components, start=10):
            conn.execute(
                "INSERT INTO ASSEMBLY_BOM_LINE (ASSEMBLY_ID, FIND_NUMBER, CHILD_PART_ID, QUANTITY, UNIT_ID, LINE_NOTE) VALUES (?, ?, ?, ?, ?, ?)",
                (assembly["ASSEMBLY_ID"], find_number, child["PART_ID"], rng.choice([1,1,1,2,3,4]), unit_ids["EA"], rng.choice(["primary", "redundant", "cold spare", "instrument bay"]))
            )
        chosen_supplies = rng.sample(supply_rows, k=rng.randint(3, 5))
        for s in chosen_supplies:
            unit_id = s["UNIT_ID"]
            qty = rng.choice([0.25, 0.5, 1, 2, 3, 5]) if unit_id != unit_ids["EA"] else rng.choice([1,2,3])
            conn.execute(
                "INSERT INTO ASSEMBLY_SUPPLY_REQUIREMENT (ASSEMBLY_ID, SUPPLY_ID, QUANTITY, UNIT_ID, USE_CONTEXT) VALUES (?, ?, ?, ?, ?)",
                (assembly["ASSEMBLY_ID"], s["SUPPLY_ID"], qty, unit_id, rng.choice(["integration", "sterile prep", "field servicing", "pack out"]))
            )

    conn.commit()
    return {
        "suppliers": conn.execute("SELECT COUNT(*) FROM SUPPLIER").fetchone()[0],
        "components": conn.execute("SELECT COUNT(*) FROM PART p JOIN PART_TYPE pt ON pt.PART_TYPE_ID=p.PART_TYPE_ID WHERE pt.PART_TYPE_CODE='COMPONENT'").fetchone()[0],
        "assemblies": conn.execute("SELECT COUNT(*) FROM ASSEMBLY").fetchone()[0],
        "supplies": conn.execute("SELECT COUNT(*) FROM SUPPLY").fetchone()[0],
        "bom_lines": conn.execute("SELECT COUNT(*) FROM ASSEMBLY_BOM_LINE").fetchone()[0],
        "assembly_supply_requirements": conn.execute("SELECT COUNT(*) FROM ASSEMBLY_SUPPLY_REQUIREMENT").fetchone()[0],
    }

# ---------------------------
# Document generation
# ---------------------------

DOC_EXT = {
    "ENGINEERING_SPEC": ".md",
    "SUPPLIER_QUOTE": ".txt",
    "EMAIL": ".txt",
    "MAINTENANCE_NOTE": ".txt",
    "TEST_REPORT": ".md",
    "PRICE_LIST": ".csv",
}


def get_all_aliases(conn: sqlite3.Connection) -> tuple[dict[int, list[str]], dict[int, list[str]], dict[int, list[str]], dict[int, list[str]], dict[int, list[str]]]:
    part_aliases = defaultdict(list)
    for row in conn.execute("SELECT PART_ID, ALIAS_TEXT FROM PART_ALIAS"):
        part_aliases[row["PART_ID"]].append(row["ALIAS_TEXT"])
    supply_aliases = defaultdict(list)
    for row in conn.execute("SELECT SUPPLY_ID, ALIAS_TEXT FROM SUPPLY_ALIAS"):
        supply_aliases[row["SUPPLY_ID"]].append(row["ALIAS_TEXT"])
    supplier_aliases = defaultdict(list)
    for row in conn.execute("SELECT SUPPLIER_ID, ALIAS_TEXT FROM SUPPLIER_ALIAS"):
        supplier_aliases[row["SUPPLIER_ID"]].append(row["ALIAS_TEXT"])
    supplier_part_numbers = defaultdict(list)
    for row in conn.execute("SELECT PART_ID, SUPPLIER_PART_NUMBER FROM SUPPLIER_PART"):
        supplier_part_numbers[row["PART_ID"]].append(row["SUPPLIER_PART_NUMBER"])
    supplier_supply_numbers = defaultdict(list)
    for row in conn.execute("SELECT SUPPLY_ID, SUPPLIER_SUPPLY_NUMBER FROM SUPPLIER_SUPPLY"):
        supplier_supply_numbers[row["SUPPLY_ID"]].append(row["SUPPLIER_SUPPLY_NUMBER"])
    return part_aliases, supply_aliases, supplier_aliases, supplier_part_numbers, supplier_supply_numbers


def choose_surface_form(kind: str, row: sqlite3.Row | dict[str, Any], aliases: list[str], rng: random.Random, supplier_numbers: list[str] | None = None) -> str:
    options = []
    if kind == "PART":
        options.extend([row["CANONICAL_NAME"], row["PART_NUMBER"]])
    elif kind == "ASSEMBLY":
        options.extend([row["CANONICAL_NAME"], row["ASSEMBLY_CODE"], row["PART_NUMBER"]])
    elif kind == "SUPPLY":
        options.extend([row["CANONICAL_NAME"], row["SUPPLY_CODE"]])
    elif kind == "SUPPLIER":
        options.append(row["CANONICAL_NAME"])
    options.extend(aliases)
    if supplier_numbers:
        options.extend(supplier_numbers)
    options = [o for o in options if o]
    return rng.choice(options)


def generate_doc_content(doc_type: str, assembly: dict[str, Any], components: list[dict[str, Any]], supplies: list[dict[str, Any]], suppliers: list[dict[str, Any]], surface: dict[str, str], source: str, model_name: str | None, rng: random.Random, ollama: OllamaClient | None, *, conn: sqlite3.Connection | None = None, run_context: str | None = None, generated_document_id: int | None = None) -> str:
    if source in {"fast", "quality"} and ollama and model_name:
        prompt = render_prompt(
            "generator/doc_generation.prompt.txt",
            """
            Write a messy but plausible operational $doc_type for a Europa mission assembly.
            Assembly: $assembly_surface ($assembly_name)
            Include 4-6 components, 2-3 supplies, one supplier reference, and one small inconsistency or shorthand naming issue.
            Components: $components
            Supplies: $supplies
            Supplier refs: $suppliers
            Keep under 350 words. No markdown code fences.
            """,
            doc_type=doc_type.replace("_", " ").lower(),
            assembly_surface=surface["assembly"],
            assembly_name=assembly["CANONICAL_NAME"],
            components=", ".join(surface[f"part_{i}"] for i in range(min(len(components), 5))),
            supplies=", ".join(surface[f"supply_{i}"] for i in range(min(len(supplies), 3))),
            suppliers=", ".join(surface[f"supplier_{i}"] for i in range(min(len(suppliers), 2))),
        )
        try:
            text = ollama.generate(model_name, prompt, temperature=0.4 if source == "fast" else 0.25)
            if conn is not None:
                log_llm_call(conn, "GENERATE_DOC", "DOC_GENERATOR", model_name, run_context=run_context, generated_document_id=generated_document_id, prompt_text=prompt, response_text=text, success_flag=bool(text))
            if text:
                return text.strip()
        except Exception as exc:
            if conn is not None:
                log_llm_call(conn, "GENERATE_DOC", "DOC_GENERATOR", model_name, run_context=run_context, generated_document_id=generated_document_id, prompt_text=prompt, response_text=str(exc), success_flag=False)

    part_lines = [f"- {surface[f'part_{i}']} qty {components[i]['QUANTITY']} ea" for i in range(len(components))]
    supply_lines = [f"- {surface[f'supply_{i}']} qty {supplies[i]['QUANTITY']} {supplies[i]['UNIT_CODE'].lower()} ({supplies[i]['USE_CONTEXT']})" for i in range(len(supplies))]
    supplier_text = ", ".join(surface[f'supplier_{i}'] for i in range(len(suppliers)))

    if doc_type == "ENGINEERING_SPEC":
        return textwrap.dedent(f"""
        # Engineering Specification
        Assembly reference: {surface['assembly']}
        Mission phase: {assembly['MISSION_PHASE']}
        Criticality: {assembly['CRITICALITY']}

        Required component stack:
        {os.linesep.join(part_lines)}

        Required consumables:
        {os.linesep.join(supply_lines)}

        Approved vendor context: {supplier_text}
        Note: use rev-controlled hardware where practical; older notes may still mention {components[0]['PART_NUMBER'].replace('CMP', 'C')}.
        """).strip()
    if doc_type == "SUPPLIER_QUOTE":
        rows = ["line,item,qty,unit_price,currency,supplier_ref"]
        for i, p in enumerate(components, start=1):
            price = round(rng.uniform(800, 4200), 2)
            rows.append(f"{i},{surface[f'part_{i-1}']},{p['QUANTITY']},{price},USD,{surface['supplier_0']}")
        return "\n".join(rows)
    if doc_type == "PRICE_LIST":
        rows = ["sku,description,pack_basis,unit_price,currency"]
        for i, s in enumerate(supplies, start=1):
            rows.append(f"{surface[f'supply_{i-1}']},{s['CANONICAL_NAME']},{rng.choice(['unit','box','kit'])},{round(rng.uniform(5,160),2)},USD")
        return "\n".join(rows)
    if doc_type == "EMAIL":
        return textwrap.dedent(f"""
        Subject: open items for {surface['assembly']}

        Team,
        we still need confirmation on {surface['part_0']} and {surface['part_1']} before integration.
        Procurement says {surface['supplier_0']} can ship the valve stack but the note still references {components[0]['PART_NUMBER']} rev {components[0]['REVISION']}.
        For sterile prep, please reserve {surface['supply_0']} and {surface['supply_1']}.
        Also confirm whether {surface['assembly']} is using the same manifold naming as last week's quote.
        """).strip()
    if doc_type == "MAINTENANCE_NOTE":
        return textwrap.dedent(f"""
        Field servicing note for {surface['assembly']}.
        Observed issues with {surface['part_0']} during cold soak cycle.
        Replaced seal interface using {surface['supply_0']} and wipe-down with {surface['supply_1']}.
        Legacy shorthand still in circulation: {components[0]['PART_NUMBER'].replace('CMP','C')} / {surface['part_0']}.
        Vendor tag on removed unit reads {surface['supplier_0']}.
        """).strip()
    if doc_type == "TEST_REPORT":
        return textwrap.dedent(f"""
        # Test Report
        Unit under test: {surface['assembly']}
        Verified components:
        {os.linesep.join(part_lines[:max(3, len(part_lines)-1)])}

        Consumables used:
        {os.linesep.join(supply_lines)}

        Result: pass with one naming discrepancy between spec text and supplier reference.
        Supplier evidence: {supplier_text}
        """).strip()
    raise ValueError(doc_type)


def generate_documents(conn: sqlite3.Connection, root: Path, cfg: dict[str, Any], run_label: str) -> dict[str, Any]:
    docs_root = ensure_dir(root / "docs" / run_label)
    rng = random.Random(int(cfg.get("seed", 42)) + 1000)
    doc_type_ids = {r["TYPE_CODE"]: r["DOCUMENT_TYPE_ID"] for r in conn.execute("SELECT * FROM DOCUMENT_TYPE")}

    existing = conn.execute("SELECT GENERATED_RUN_ID FROM GENERATED_RUN WHERE RUN_LABEL = ?", (run_label,)).fetchone()
    if existing:
        raise ValueError(f"Run label already exists: {run_label}")
    conn.execute(
        "INSERT INTO GENERATED_RUN (RUN_LABEL, ROOT_DIR, CONFIG_JSON, CREATED_AT) VALUES (?, ?, ?, ?)",
        (run_label, str(root), json.dumps(cfg), UTC_NOW()),
    )
    generated_run_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    assemblies = [dict(r) for r in conn.execute(
        "SELECT a.ASSEMBLY_ID, a.ASSEMBLY_CODE, a.MISSION_PHASE, a.CRITICALITY, p.PART_ID, p.PART_NUMBER, p.CANONICAL_NAME, p.REVISION FROM ASSEMBLY a JOIN PART p ON p.PART_ID = a.PART_ID ORDER BY a.ASSEMBLY_ID"
    )]
    package_count = min(int(cfg.get("package_count", len(assemblies))), len(assemblies))
    assemblies = assemblies[:package_count]

    part_aliases, supply_aliases, supplier_aliases, supplier_part_numbers, supplier_supply_numbers = get_all_aliases(conn)
    docs_per_package = int(cfg.get("docs_per_package", 6))
    source_mix = cfg.get("source_mix", {"template": 90, "fast": 8, "quality": 2})
    source_choices = weighted_source_mix(source_mix, package_count * docs_per_package, rng)
    source_idx = 0
    configured_doc_types = cfg.get("document_types", ["ENGINEERING_SPEC", "SUPPLIER_QUOTE", "EMAIL", "MAINTENANCE_NOTE", "TEST_REPORT", "PRICE_LIST"])
    doc_types = configured_doc_types[:docs_per_package]
    ollama = OllamaClient(cfg.get("ollama_url", "http://localhost:11434"), timeout_sec=int(cfg.get("ollama_timeout_sec", 120)))

    counts = Counter()
    for package_num, assembly in enumerate(assemblies, start=1):
        package_id = f"PKG-{package_num:03d}"
        log_event(f"GENERATE: package {package_num}/{package_count} | package_id={package_id} | assembly={assembly['ASSEMBLY_CODE']}")
        package_dir = ensure_dir(docs_root / package_id)
        bom_rows = [dict(r) for r in conn.execute(
            "SELECT bl.ASSEMBLY_BOM_LINE_ID, bl.FIND_NUMBER, bl.QUANTITY, u.UNIT_CODE, p.PART_ID, p.PART_NUMBER, p.CANONICAL_NAME, p.REVISION FROM ASSEMBLY_BOM_LINE bl JOIN PART p ON p.PART_ID = bl.CHILD_PART_ID JOIN UNIT u ON u.UNIT_ID = bl.UNIT_ID WHERE bl.ASSEMBLY_ID = ? ORDER BY bl.FIND_NUMBER",
            (assembly["ASSEMBLY_ID"],),
        )]
        supply_rows = [dict(r) for r in conn.execute(
            "SELECT ar.ASSEMBLY_SUPPLY_REQUIREMENT_ID, ar.QUANTITY, ar.USE_CONTEXT, u.UNIT_CODE, s.SUPPLY_ID, s.SUPPLY_CODE, s.CANONICAL_NAME FROM ASSEMBLY_SUPPLY_REQUIREMENT ar JOIN SUPPLY s ON s.SUPPLY_ID = ar.SUPPLY_ID JOIN UNIT u ON u.UNIT_ID = ar.UNIT_ID WHERE ar.ASSEMBLY_ID = ? ORDER BY s.SUPPLY_CODE",
            (assembly["ASSEMBLY_ID"],),
        )]
        supplier_rows = [dict(r) for r in conn.execute(
            "SELECT DISTINCT sp.SUPPLIER_ID, sup.SUPPLIER_CODE, sup.CANONICAL_NAME FROM SUPPLIER_PART sp JOIN SUPPLIER sup ON sup.SUPPLIER_ID = sp.SUPPLIER_ID WHERE sp.PART_ID IN (SELECT CHILD_PART_ID FROM ASSEMBLY_BOM_LINE WHERE ASSEMBLY_ID = ?) LIMIT 3",
            (assembly["ASSEMBLY_ID"],),
        )]
        if not supplier_rows:
            supplier_rows = [dict(r) for r in conn.execute("SELECT SUPPLIER_ID, SUPPLIER_CODE, CANONICAL_NAME FROM SUPPLIER LIMIT 2")]

        for doc_index, doc_type in enumerate(doc_types, start=1):
            source = source_choices[source_idx]
            log_event(f"GENERATE: package {package_num}/{package_count} | doc {doc_index}/{len(doc_types)} | type={doc_type} | source={source}")
            source_idx += 1
            model_name = None
            if source == "fast":
                model_name = cfg.get("fast_model")
            elif source == "quality":
                model_name = cfg.get("quality_model")
            if source == "template":
                model_name = None

            chosen_components = rng.sample(bom_rows, k=min(len(bom_rows), rng.randint(4, min(6, len(bom_rows)))))
            chosen_supplies = rng.sample(supply_rows, k=min(len(supply_rows), rng.randint(2, min(3, len(supply_rows)))))
            chosen_suppliers = rng.sample(supplier_rows, k=min(len(supplier_rows), rng.randint(1, min(2, len(supplier_rows)))))

            surface: dict[str, str] = {"assembly": choose_surface_form("ASSEMBLY", assembly, part_aliases[assembly["PART_ID"]], rng)}
            for i, p in enumerate(chosen_components):
                surface[f"part_{i}"] = choose_surface_form("PART", p, part_aliases[p["PART_ID"]], rng, supplier_part_numbers[p["PART_ID"]])
            for i, s in enumerate(chosen_supplies):
                surface[f"supply_{i}"] = choose_surface_form("SUPPLY", s, supply_aliases[s["SUPPLY_ID"]], rng, supplier_supply_numbers[s["SUPPLY_ID"]])
            for i, sup in enumerate(chosen_suppliers):
                surface[f"supplier_{i}"] = choose_surface_form("SUPPLIER", sup, supplier_aliases[sup["SUPPLIER_ID"]], rng)

            ext = DOC_EXT[doc_type]
            doc_id = f"{package_id}_{doc_type}_{doc_index:02d}"
            file_path = package_dir / f"{doc_id}{ext}"
            conn.execute(
                "INSERT INTO GENERATED_DOCUMENT (GENERATED_RUN_ID, PACKAGE_ID, DOCUMENT_TYPE_ID, DOC_ID, GENERATION_SOURCE, MODEL_NAME, ASSEMBLY_ID, FILE_PATH, CONTENT_TEXT, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (generated_run_id, package_id, doc_type_ids[doc_type], doc_id, source.upper(), model_name, assembly["ASSEMBLY_ID"], str(file_path), "", UTC_NOW()),
            )
            generated_document_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            content = generate_doc_content(doc_type, assembly, chosen_components, chosen_supplies, chosen_suppliers, surface, source, model_name, rng, ollama if source != "template" else None, conn=conn, run_context=run_label, generated_document_id=generated_document_id)
            file_path.write_text(content, encoding="utf-8")
            conn.execute("UPDATE GENERATED_DOCUMENT SET CONTENT_TEXT = ? WHERE GENERATED_DOCUMENT_ID = ?", (content, generated_document_id))
            counts[source] += 1

            conn.execute(
                "INSERT INTO GENERATED_DOCUMENT_ENTITY (GENERATED_DOCUMENT_ID, ENTITY_KIND, ASSEMBLY_ID, ROLE_CODE, SURFACE_FORM, EVIDENCE_SOURCE) VALUES (?, 'ASSEMBLY', ?, 'PACKAGE_ROOT', ?, ?)",
                (generated_document_id, assembly["ASSEMBLY_ID"], surface["assembly"], doc_type),
            )
            for i, p in enumerate(chosen_components):
                conn.execute(
                    "INSERT INTO GENERATED_DOCUMENT_ENTITY (GENERATED_DOCUMENT_ID, ENTITY_KIND, PART_ID, ROLE_CODE, SURFACE_FORM, EVIDENCE_SOURCE) VALUES (?, 'PART', ?, ?, ?, ?)",
                    (generated_document_id, p["PART_ID"], f"COMPONENT_{i+1}", surface[f"part_{i}"], doc_type),
                )
            for i, s in enumerate(chosen_supplies):
                conn.execute(
                    "INSERT INTO GENERATED_DOCUMENT_ENTITY (GENERATED_DOCUMENT_ID, ENTITY_KIND, SUPPLY_ID, ROLE_CODE, SURFACE_FORM, EVIDENCE_SOURCE) VALUES (?, 'SUPPLY', ?, ?, ?, ?)",
                    (generated_document_id, s["SUPPLY_ID"], f"SUPPLY_{i+1}", surface[f"supply_{i}"], doc_type),
                )
            for i, sup in enumerate(chosen_suppliers):
                conn.execute(
                    "INSERT INTO GENERATED_DOCUMENT_ENTITY (GENERATED_DOCUMENT_ID, ENTITY_KIND, SUPPLIER_ID, ROLE_CODE, SURFACE_FORM, EVIDENCE_SOURCE) VALUES (?, 'SUPPLIER', ?, ?, ?, ?)",
                    (generated_document_id, sup["SUPPLIER_ID"], f"SUPPLIER_{i+1}", surface[f"supplier_{i}"], doc_type),
                )
    conn.commit()
    return {
        "generated_run_id": generated_run_id,
        "run_label": run_label,
        "documents_created": sum(counts.values()),
        "packages": package_count,
        "sources": dict(counts),
    }

# ---------------------------
# NER and resolution
# ---------------------------


def collect_resolver_options(index: dict[str, list[Candidate]], mention_text: str, max_options: int = 6) -> list[Candidate]:
    mention_norm = normalize_text(mention_text)
    candidates: list[Candidate] = []
    seen: set[tuple[str, int]] = set()
    for key, cands in index.items():
        sim = score_text_similarity(mention_norm, key)
        if sim < 0.22 and mention_norm not in key and key not in mention_norm:
            continue
        best = sorted(cands, key=lambda c: (candidate_rank_score(c, mention_text, sim), c.score), reverse=True)[0]
        dedupe = (best.kind, best.entity_id)
        if dedupe in seen:
            continue
        seen.add(dedupe)
        boosted = min(0.99, max(best.score, 0.45 + sim * 0.45 + max(0.0, candidate_rank_score(best, mention_text, sim) - max(best.score, sim))))
        candidates.append(Candidate(best.kind, best.entity_id, mention_text, best.canonical_text, best.method + '_OPTION', boosted, best.evidence | {'similarity': sim}))
    candidates.sort(key=lambda c: c.score, reverse=True)
    return candidates[:max_options]


def should_send_to_resolver_llm(candidate: Candidate | None, mention_text: str, cfg: dict[str, Any]) -> bool:
    if not bool(cfg.get("resolver_llm_enabled", True)):
        return False
    min_len = int(cfg.get('resolver_llm_min_mention_len', 4))
    if len(mention_text.strip()) < min_len:
        return False
    if candidate is None:
        return True
    if candidate.method.endswith('FALLBACK'):
        return True
    if candidate.score < float(cfg.get('resolver_llm_score_threshold', 0.9)):
        return True
    review_methods = set(cfg.get('resolver_llm_review_methods', ['CANONICAL_NAME_RANKED','CANONICAL_NAME_FALLBACK','PART_ALIAS_SHORT_NAME_FALLBACK','NAME_VARIANT_FALLBACK']))
    return candidate.method in review_methods


def resolver_llm_decide(client: OllamaClient, model_name: str, mention_text: str, context: str, options: list[Candidate]) -> tuple[Candidate | None, str, str]:
    option_lines = []
    for idx, cand in enumerate(options, start=1):
        option_lines.append(f"{idx}. kind={cand.kind} entity_id={cand.entity_id} canonical={cand.canonical_text} method={cand.method} score={cand.score:.3f} evidence={json.dumps(cand.evidence)}")
    prompt = render_prompt(
        "resolver/resolver_decide.prompt.txt",
        """
        You are an entity resolution specialist.
        Decide whether the mention below maps to one of the candidate entities.
        Return JSON only with keys: choose_index, confidence, reason.
        choose_index must be an integer 1-$max_choice or 0 for none.

        Mention: $mention_text
        Context: $context

        Candidates:
        $candidate_lines
        """,
        max_choice=len(options),
        mention_text=mention_text,
        context=context,
        candidate_lines=os.linesep.join(option_lines),
    )
    response = client.generate(model_name, prompt, temperature=0.05, keep_alive='30m')
    parsed = safe_extract_json_block(response) or {}
    chosen = int(parsed.get('choose_index', 0) or 0)
    if 1 <= chosen <= len(options):
        cand = options[chosen - 1]
        boosted = Candidate(cand.kind, cand.entity_id, mention_text, cand.canonical_text, 'RESOLVER_LLM', max(cand.score, float(parsed.get('confidence', 0.8) or 0.8)), cand.evidence | {'llm_reason': parsed.get('reason')})
        return boosted, prompt, response
    return None, prompt, response

@dataclass
class Candidate:
    kind: str
    entity_id: int
    text: str
    canonical_text: str
    method: str
    score: float
    evidence: dict[str, Any]


def mention_looks_like_assembly(mention_text: str) -> bool:
    cleaned = mention_text.strip()
    upper = cleaned.upper()
    norm = normalize_text(cleaned)
    if re.fullmatch(r"ASM-\d{3,4}", upper):
        return True
    if re.fullmatch(r"A-?\d{3,4}", upper):
        return True
    return "assembly" in norm or " assy " in f" {norm} " or " module " in f" {norm} "


def candidate_rank_score(candidate: Candidate, mention_text: str, similarity: float | None = None) -> float:
    score = max(candidate.score, similarity) if similarity is not None else candidate.score
    if mention_looks_like_assembly(mention_text):
        if candidate.kind == "ASSEMBLY":
            score += 0.12
            if candidate.method == "ASSEMBLY_CODE":
                score += 0.06
            elif candidate.method == "ASSEMBLY_PART_NUMBER":
                score += 0.05
            elif candidate.method == "ASSEMBLY_NAME":
                score += 0.04
            elif candidate.method.startswith("ASSEMBLY_ALIAS_"):
                score += 0.03
        elif candidate.kind == "PART" and candidate.evidence.get("is_assembly_part"):
            score -= 0.12
    return score


def build_index(conn: sqlite3.Connection) -> dict[str, list[Candidate]]:
    index: dict[str, list[Candidate]] = defaultdict(list)
    for row in conn.execute("SELECT PART_ID, CANONICAL_NAME, PART_NUMBER, CASE WHEN PART_NUMBER LIKE 'ASM-%' THEN 1 ELSE 0 END AS IS_ASSEMBLY_PART FROM PART"):
        part_id = row["PART_ID"]
        index[normalize_text(row["CANONICAL_NAME"])].append(Candidate("PART", part_id, row["CANONICAL_NAME"], row["CANONICAL_NAME"], "CANONICAL_NAME", 0.96, {"canonical_name": row["CANONICAL_NAME"], "is_assembly_part": bool(row["IS_ASSEMBLY_PART"])}))
        index[normalize_text(row["PART_NUMBER"])].append(Candidate("PART", part_id, row["PART_NUMBER"], row["CANONICAL_NAME"], "DIRECT_PART_NUMBER", 0.99, {"part_number": row["PART_NUMBER"], "is_assembly_part": bool(row["IS_ASSEMBLY_PART"])}))
    for row in conn.execute("SELECT pa.PART_ID, pa.ALIAS_TEXT, pa.ALIAS_KIND, p.CANONICAL_NAME, CASE WHEN p.PART_NUMBER LIKE 'ASM-%' THEN 1 ELSE 0 END AS IS_ASSEMBLY_PART FROM PART_ALIAS pa JOIN PART p ON p.PART_ID = pa.PART_ID"):
        index[normalize_text(row["ALIAS_TEXT"])].append(Candidate("PART", row["PART_ID"], row["ALIAS_TEXT"], row["CANONICAL_NAME"], f"PART_ALIAS_{row['ALIAS_KIND']}", 0.88, {"alias": row["ALIAS_TEXT"], "is_assembly_part": bool(row["IS_ASSEMBLY_PART"])}))
    for row in conn.execute("SELECT a.ASSEMBLY_ID, a.ASSEMBLY_CODE, p.PART_NUMBER, p.CANONICAL_NAME FROM ASSEMBLY a JOIN PART p ON p.PART_ID = a.PART_ID"):
        index[normalize_text(row["ASSEMBLY_CODE"])].append(Candidate("ASSEMBLY", row["ASSEMBLY_ID"], row["ASSEMBLY_CODE"], row["CANONICAL_NAME"], "ASSEMBLY_CODE", 1.0, {"assembly_code": row["ASSEMBLY_CODE"]}))
        index[normalize_text(row["PART_NUMBER"])].append(Candidate("ASSEMBLY", row["ASSEMBLY_ID"], row["PART_NUMBER"], row["CANONICAL_NAME"], "ASSEMBLY_PART_NUMBER", 0.995, {"assembly_part_number": row["PART_NUMBER"], "assembly_code": row["ASSEMBLY_CODE"]}))
        index[normalize_text(row["CANONICAL_NAME"])].append(Candidate("ASSEMBLY", row["ASSEMBLY_ID"], row["CANONICAL_NAME"], row["CANONICAL_NAME"], "ASSEMBLY_NAME", 0.975, {"assembly_code": row["ASSEMBLY_CODE"]}))
    for row in conn.execute("SELECT a.ASSEMBLY_ID, pa.ALIAS_TEXT, pa.ALIAS_KIND, a.ASSEMBLY_CODE, p.PART_NUMBER, p.CANONICAL_NAME FROM ASSEMBLY a JOIN PART p ON p.PART_ID = a.PART_ID JOIN PART_ALIAS pa ON pa.PART_ID = p.PART_ID"):
        index[normalize_text(row["ALIAS_TEXT"])].append(Candidate("ASSEMBLY", row["ASSEMBLY_ID"], row["ALIAS_TEXT"], row["CANONICAL_NAME"], f"ASSEMBLY_ALIAS_{row['ALIAS_KIND']}", 0.93, {"alias": row["ALIAS_TEXT"], "assembly_code": row["ASSEMBLY_CODE"], "assembly_part_number": row["PART_NUMBER"]}))
    for row in conn.execute("SELECT SUPPLY_ID, CANONICAL_NAME, SUPPLY_CODE FROM SUPPLY"):
        index[normalize_text(row["CANONICAL_NAME"])].append(Candidate("SUPPLY", row["SUPPLY_ID"], row["CANONICAL_NAME"], row["CANONICAL_NAME"], "CANONICAL_NAME", 0.96, {"canonical_name": row["CANONICAL_NAME"]}))
        index[normalize_text(row["SUPPLY_CODE"])].append(Candidate("SUPPLY", row["SUPPLY_ID"], row["SUPPLY_CODE"], row["CANONICAL_NAME"], "DIRECT_SUPPLY_CODE", 0.99, {"supply_code": row["SUPPLY_CODE"]}))
    for row in conn.execute("SELECT sa.SUPPLY_ID, sa.ALIAS_TEXT, sa.ALIAS_KIND, s.CANONICAL_NAME FROM SUPPLY_ALIAS sa JOIN SUPPLY s ON s.SUPPLY_ID = sa.SUPPLY_ID"):
        index[normalize_text(row["ALIAS_TEXT"])].append(Candidate("SUPPLY", row["SUPPLY_ID"], row["ALIAS_TEXT"], row["CANONICAL_NAME"], f"SUPPLY_ALIAS_{row['ALIAS_KIND']}", 0.87, {"alias": row["ALIAS_TEXT"]}))
    for row in conn.execute("SELECT SUPPLIER_ID, CANONICAL_NAME, SUPPLIER_CODE FROM SUPPLIER"):
        index[normalize_text(row["CANONICAL_NAME"])].append(Candidate("SUPPLIER", row["SUPPLIER_ID"], row["CANONICAL_NAME"], row["CANONICAL_NAME"], "CANONICAL_NAME", 0.97, {"supplier_name": row["CANONICAL_NAME"]}))
        index[normalize_text(row["SUPPLIER_CODE"])].append(Candidate("SUPPLIER", row["SUPPLIER_ID"], row["SUPPLIER_CODE"], row["CANONICAL_NAME"], "SUPPLIER_CODE", 0.99, {"supplier_code": row["SUPPLIER_CODE"]}))
    for row in conn.execute("SELECT sa.SUPPLIER_ID, sa.ALIAS_TEXT, sa.ALIAS_KIND, s.CANONICAL_NAME FROM SUPPLIER_ALIAS sa JOIN SUPPLIER s ON s.SUPPLIER_ID = sa.SUPPLIER_ID"):
        index[normalize_text(row["ALIAS_TEXT"])].append(Candidate("SUPPLIER", row["SUPPLIER_ID"], row["ALIAS_TEXT"], row["CANONICAL_NAME"], f"SUPPLIER_ALIAS_{row['ALIAS_KIND']}", 0.88, {"alias": row["ALIAS_TEXT"]}))
    for row in conn.execute("SELECT PART_ID, SUPPLIER_PART_NUMBER FROM SUPPLIER_PART"):
        index[normalize_text(row["SUPPLIER_PART_NUMBER"])].append(Candidate("PART", row["PART_ID"], row["SUPPLIER_PART_NUMBER"], row["SUPPLIER_PART_NUMBER"], "SUPPLIER_PART_NUMBER", 0.95, {"supplier_part_number": row["SUPPLIER_PART_NUMBER"]}))
    for row in conn.execute("SELECT SUPPLY_ID, SUPPLIER_SUPPLY_NUMBER FROM SUPPLIER_SUPPLY"):
        index[normalize_text(row["SUPPLIER_SUPPLY_NUMBER"])].append(Candidate("SUPPLY", row["SUPPLY_ID"], row["SUPPLIER_SUPPLY_NUMBER"], row["SUPPLIER_SUPPLY_NUMBER"], "SUPPLIER_SUPPLY_NUMBER", 0.95, {"supplier_supply_number": row["SUPPLIER_SUPPLY_NUMBER"]}))
    return index


def resolve_candidate(index: dict[str, list[Candidate]], mention_text: str) -> Candidate | None:
    key = normalize_text(mention_text)
    if key in index and len(index[key]) == 1:
        return index[key][0]
    if key in index and len(index[key]) > 1:
        ranked = sorted(index[key], key=lambda c: (candidate_rank_score(c, mention_text), c.score), reverse=True)
        cand = ranked[0]
        adjusted = min(0.99, candidate_rank_score(cand, mention_text) - 0.02)
        return Candidate(cand.kind, cand.entity_id, cand.text, cand.canonical_text, cand.method + "_RANKED", adjusted, cand.evidence | {"candidate_count": len(ranked)})
    # fallback similarity
    best: Candidate | None = None
    best_rank = -1.0
    for norm, cands in index.items():
        sim = score_text_similarity(key, norm)
        if sim >= 0.6:
            cand = sorted(cands, key=lambda c: (candidate_rank_score(c, mention_text, sim), c.score), reverse=True)[0]
            rank_score = candidate_rank_score(cand, mention_text, sim)
            score = round(min(0.99, 0.55 + sim * 0.35 + max(0.0, rank_score - max(cand.score, sim))), 3)
            if best is None or rank_score > best_rank:
                best_rank = rank_score
                best = Candidate(cand.kind, cand.entity_id, mention_text, cand.canonical_text, cand.method + "_FALLBACK", score, cand.evidence | {"similarity": sim})
    return best


def extract_mentions_from_text(text: str) -> list[tuple[str, int, int]]:
    patterns = [
        r"\b(?:CMP|ASM|SUPPLY)-\d{3,4}\b",
        r"\b(?:C|A)-?\d{3,4}\b",
        r"\bV-\d+-[A-Z]{3}-\d{3,4}\b",
        r"\bS-\d+-SUPPLY-\d{3}\b",
        r"\b[A-Z][A-Za-z0-9\-/]{2,}(?: [A-Z][A-Za-z0-9\-/]{2,}){0,4}\b",
    ]
    seen = set()
    results = []
    for pat in patterns:
        for m in re.finditer(pat, text):
            mention = m.group(0).strip(" ,.;:\n\t")
            if len(mention) < 3:
                continue
            key = (mention.lower(), m.start(), m.end())
            if key in seen:
                continue
            seen.add(key)
            results.append((mention, m.start(), m.end()))
    return sorted(results, key=lambda x: x[1])


def run_ner(conn: sqlite3.Connection, root: Path, cfg: dict[str, Any], docs_run: str) -> dict[str, Any]:
    generated_run = conn.execute("SELECT * FROM GENERATED_RUN WHERE RUN_LABEL = ?", (docs_run,)).fetchone()
    if not generated_run:
        raise ValueError(f"Generated run not found: {docs_run}")
    ner_run_label = f"NER_{docs_run}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    conn.execute(
        "INSERT INTO NER_RUN (RUN_LABEL, GENERATED_RUN_ID, CONFIG_JSON, CREATED_AT) VALUES (?, ?, ?, ?)",
        (ner_run_label, generated_run["GENERATED_RUN_ID"], json.dumps(cfg), UTC_NOW()),
    )
    ner_run_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    index = build_index(conn)
    docs = [dict(r) for r in conn.execute("SELECT gd.*, dt.TYPE_CODE FROM GENERATED_DOCUMENT gd JOIN DOCUMENT_TYPE dt ON dt.DOCUMENT_TYPE_ID = gd.DOCUMENT_TYPE_ID WHERE gd.GENERATED_RUN_ID = ? ORDER BY gd.GENERATED_DOCUMENT_ID", (generated_run["GENERATED_RUN_ID"],))]

    resolver_model = cfg.get("resolver_model")
    if not resolver_model:
        raise ValueError("resolver_model must be configured in this release")
    resolver_client = OllamaClient(cfg.get("ollama_url", "http://localhost:11434"), timeout_sec=int(cfg.get("ollama_timeout_sec", 180)))

    methods = Counter()
    resolved = unresolved = raw_mentions = 0
    resolver_llm_call_count = 0

    for doc_idx, doc in enumerate(docs, start=1):
        log_event(f"NER: document {doc_idx}/{len(docs)} | doc_id={doc['DOC_ID']}")
        text = doc["CONTENT_TEXT"]
        mentions = extract_mentions_from_text(text)
        for mention_text, start, end in mentions:
            raw_mentions += 1
            context = text[max(0, start-80): min(len(text), end+80)]
            guess = "UNKNOWN"
            if mention_looks_like_assembly(mention_text):
                guess = "ASSEMBLY"
            elif mention_text.startswith(("CMP", "C-")):
                guess = "PART"
            elif mention_text.startswith(("SUPPLY", "S-")):
                guess = "SUPPLY"
            elif any(word in mention_text.lower() for word in ["dynamics", "forge", "works", "lab", "systems"]):
                guess = "SUPPLIER"
            conn.execute(
                "INSERT INTO NER_MENTION (NER_RUN_ID, GENERATED_DOCUMENT_ID, MENTION_TEXT, START_OFFSET, END_OFFSET, ENTITY_KIND_GUESS, EXTRACTION_METHOD, CONTEXT_SNIPPET) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (ner_run_id, doc["GENERATED_DOCUMENT_ID"], mention_text, start, end, guess, "REGEX_SCAN", context),
            )
            mention_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            cand = resolve_candidate(index, mention_text)
            if should_send_to_resolver_llm(cand, mention_text, cfg):
                options = collect_resolver_options(index, mention_text, max_options=int(cfg.get('resolver_llm_max_options', 6)))
                if options:
                    try:
                        llm_cand, prompt_text, response_text = resolver_llm_decide(resolver_client, resolver_model, mention_text, context, options)
                        log_llm_call(conn, 'RESOLVE_MENTION', 'RESOLVER', resolver_model, run_context=ner_run_label, generated_document_id=doc['GENERATED_DOCUMENT_ID'], ner_mention_id=mention_id, prompt_text=prompt_text, response_text=response_text, success_flag=True)
                        resolver_llm_call_count += 1
                        if llm_cand is not None:
                            cand = llm_cand
                    except Exception as exc:
                        log_llm_call(conn, 'RESOLVE_MENTION', 'RESOLVER', resolver_model, run_context=ner_run_label, generated_document_id=doc['GENERATED_DOCUMENT_ID'], ner_mention_id=mention_id, prompt_text=f'mention={mention_text}', response_text=str(exc), success_flag=False)
            if cand is None:
                unresolved += 1
                conn.execute(
                    "INSERT INTO NER_CONFLICT (NER_RUN_ID, GENERATED_DOCUMENT_ID, CONFLICT_TYPE, SEVERITY, DESCRIPTION_TEXT, RELATED_MENTION_ID, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (ner_run_id, doc["GENERATED_DOCUMENT_ID"], "UNRESOLVED", "MEDIUM", f"No canonical match found for mention '{mention_text}'", mention_id, UTC_NOW()),
                )
                continue
            resolved += 1
            methods[cand.method] += 1
            provenance = "STRONG" if cand.score >= 0.95 else "MEDIUM" if cand.score >= 0.8 else "WEAK"
            part_id = supply_id = supplier_id = assembly_id = None
            if cand.kind == "PART":
                part_id = cand.entity_id
            elif cand.kind == "SUPPLY":
                supply_id = cand.entity_id
            elif cand.kind == "SUPPLIER":
                supplier_id = cand.entity_id
            elif cand.kind == "ASSEMBLY":
                assembly_id = cand.entity_id
            conn.execute(
                "INSERT INTO NER_RESOLUTION (NER_MENTION_ID, RESOLVED_ENTITY_KIND, PART_ID, SUPPLY_ID, SUPPLIER_ID, ASSEMBLY_ID, RESOLUTION_METHOD, CONFIDENCE_SCORE, PROVENANCE_STRENGTH, EVIDENCE_JSON) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (mention_id, cand.kind, part_id, supply_id, supplier_id, assembly_id, cand.method, cand.score, provenance, json.dumps(cand.evidence)),
            )

    packages = [dict(r) for r in conn.execute("SELECT DISTINCT gd.PACKAGE_ID, gd.ASSEMBLY_ID FROM GENERATED_DOCUMENT gd WHERE gd.GENERATED_RUN_ID = ? ORDER BY gd.PACKAGE_ID", (generated_run["GENERATED_RUN_ID"],))]
    component_rates = []
    supply_rates = []
    for pkg in packages:
        expected_parts = {r[0] for r in conn.execute("SELECT CHILD_PART_ID FROM ASSEMBLY_BOM_LINE WHERE ASSEMBLY_ID = ?", (pkg["ASSEMBLY_ID"],))}
        found_parts = {r[0] for r in conn.execute("SELECT DISTINCT nr.PART_ID FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID JOIN GENERATED_DOCUMENT gd ON gd.GENERATED_DOCUMENT_ID = nm.GENERATED_DOCUMENT_ID WHERE nm.NER_RUN_ID = ? AND gd.PACKAGE_ID = ? AND nr.PART_ID IS NOT NULL", (ner_run_id, pkg["PACKAGE_ID"]))}
        expected_supplies = {r[0] for r in conn.execute("SELECT SUPPLY_ID FROM ASSEMBLY_SUPPLY_REQUIREMENT WHERE ASSEMBLY_ID = ?", (pkg["ASSEMBLY_ID"],))}
        found_supplies = {r[0] for r in conn.execute("SELECT DISTINCT nr.SUPPLY_ID FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID JOIN GENERATED_DOCUMENT gd ON gd.GENERATED_DOCUMENT_ID = nm.GENERATED_DOCUMENT_ID WHERE nm.NER_RUN_ID = ? AND gd.PACKAGE_ID = ? AND nr.SUPPLY_ID IS NOT NULL", (ner_run_id, pkg["PACKAGE_ID"]))}
        comp_rate = len(expected_parts & found_parts) / max(1, len(expected_parts))
        sup_rate = len(expected_supplies & found_supplies) / max(1, len(expected_supplies))
        component_rates.append(comp_rate)
        supply_rates.append(sup_rate)
        conn.execute(
            "INSERT INTO NER_PACKAGE_COVERAGE (NER_RUN_ID, PACKAGE_ID, ASSEMBLY_ID, COMPONENT_MATCH_RATE, SUPPLY_MATCH_RATE, COMPONENT_EXPECTED_COUNT, COMPONENT_FOUND_COUNT, SUPPLY_EXPECTED_COUNT, SUPPLY_FOUND_COUNT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ner_run_id, pkg["PACKAGE_ID"], pkg["ASSEMBLY_ID"], comp_rate, sup_rate, len(expected_parts), len(expected_parts & found_parts), len(expected_supplies), len(expected_supplies & found_supplies)),
        )
        for pid in found_parts:
            mention_count = conn.execute("SELECT COUNT(*) FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID JOIN GENERATED_DOCUMENT gd ON gd.GENERATED_DOCUMENT_ID = nm.GENERATED_DOCUMENT_ID WHERE nm.NER_RUN_ID = ? AND gd.PACKAGE_ID = ? AND nr.PART_ID = ?", (ner_run_id, pkg["PACKAGE_ID"], pid)).fetchone()[0]
            conn.execute("INSERT INTO NER_RECONSTRUCTED_BOM_LINE (NER_RUN_ID, PACKAGE_ID, ASSEMBLY_ID, PART_ID, MENTION_COUNT) VALUES (?, ?, ?, ?, ?)", (ner_run_id, pkg["PACKAGE_ID"], pkg["ASSEMBLY_ID"], pid, mention_count))
        for sid in found_supplies:
            mention_count = conn.execute("SELECT COUNT(*) FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID JOIN GENERATED_DOCUMENT gd ON gd.GENERATED_DOCUMENT_ID = nm.GENERATED_DOCUMENT_ID WHERE nm.NER_RUN_ID = ? AND gd.PACKAGE_ID = ? AND nr.SUPPLY_ID = ?", (ner_run_id, pkg["PACKAGE_ID"], sid)).fetchone()[0]
            conn.execute("INSERT INTO NER_RECONSTRUCTED_SUPPLY_REQUIREMENT (NER_RUN_ID, PACKAGE_ID, ASSEMBLY_ID, SUPPLY_ID, MENTION_COUNT) VALUES (?, ?, ?, ?, ?)", (ner_run_id, pkg["PACKAGE_ID"], pkg["ASSEMBLY_ID"], sid, mention_count))
    conn.commit()
    return {
        "created_at": UTC_NOW(),
        "workspace_root": str(root),
        "db_path": str(root / "db" / "europa_masterdata.sqlite"),
        "docs_run": docs_run,
        "run_label": ner_run_label,
        "documents_processed": len(docs),
        "raw_mentions": raw_mentions,
        "resolved_mentions": resolved,
        "unresolved_mentions": unresolved,
        "distinct_parts": conn.execute("SELECT COUNT(DISTINCT PART_ID) FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ? AND nr.PART_ID IS NOT NULL", (ner_run_id,)).fetchone()[0],
        "distinct_supplies": conn.execute("SELECT COUNT(DISTINCT SUPPLY_ID) FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ? AND nr.SUPPLY_ID IS NOT NULL", (ner_run_id,)).fetchone()[0],
        "distinct_suppliers": conn.execute("SELECT COUNT(DISTINCT SUPPLIER_ID) FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ? AND nr.SUPPLIER_ID IS NOT NULL", (ner_run_id,)).fetchone()[0],
        "resolver_llm_call_count": resolver_llm_call_count,
        "resolution_methods": dict(methods),
        "package_component_match_rate_avg": round(statistics.mean(component_rates), 4) if component_rates else 0.0,
        "package_supply_match_rate_avg": round(statistics.mean(supply_rates), 4) if supply_rates else 0.0,
    }

# ---------------------------
# Auditor/Judge
# ---------------------------

def get_latest_ner_run_id(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT NER_RUN_ID FROM NER_RUN ORDER BY CREATED_AT DESC LIMIT 1").fetchone()
    if not row:
        raise ValueError("No NER run found")
    return int(row[0])


def build_audit_cases(conn: sqlite3.Connection, ner_run_id: int) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    # weak provenance
    for row in conn.execute(
        "SELECT nr.NER_RESOLUTION_ID, nm.GENERATED_DOCUMENT_ID, nm.MENTION_TEXT, nr.RESOLVED_ENTITY_KIND, nr.PART_ID, nr.SUPPLY_ID, nr.SUPPLIER_ID, nr.ASSEMBLY_ID, nr.CONFIDENCE_SCORE, nr.RESOLUTION_METHOD, gd.CONTENT_TEXT FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID JOIN GENERATED_DOCUMENT gd ON gd.GENERATED_DOCUMENT_ID = nm.GENERATED_DOCUMENT_ID WHERE nm.NER_RUN_ID = ? AND nr.CONFIDENCE_SCORE < 0.80 ORDER BY nr.CONFIDENCE_SCORE ASC LIMIT 40",
        (ner_run_id,),
    ):
        cases.append({
            "case_type": "WEAK_PROVENANCE",
            "severity": "HIGH" if row["CONFIDENCE_SCORE"] < 0.7 else "MEDIUM",
            "generated_document_id": row["GENERATED_DOCUMENT_ID"],
            "part_id": row["PART_ID"],
            "supply_id": row["SUPPLY_ID"],
            "assembly_id": row["ASSEMBLY_ID"],
            "source_resolution_id": row["NER_RESOLUTION_ID"],
            "payload": {
                "mention_text": row["MENTION_TEXT"],
                "resolved_entity_kind": row["RESOLVED_ENTITY_KIND"],
                "resolution_method": row["RESOLUTION_METHOD"],
                "confidence": row["CONFIDENCE_SCORE"],
                "document_excerpt": row["CONTENT_TEXT"][:800],
            },
        })
    # price outliers for parts
    outlier_sql = """
    WITH PR AS (
      SELECT PART_ID, AVG(BASE_UNIT_PRICE) AS AVG_P, MIN(BASE_UNIT_PRICE) AS MIN_P, MAX(BASE_UNIT_PRICE) AS MAX_P
      FROM SUPPLIER_PART GROUP BY PART_ID
    )
    SELECT p.PART_ID, p.CANONICAL_NAME, p.PART_NUMBER, pr.AVG_P, pr.MIN_P, pr.MAX_P
    FROM PR pr JOIN PART p ON p.PART_ID = pr.PART_ID
    WHERE pr.MAX_P > pr.MIN_P * 2.5
    LIMIT 20
    """
    for row in conn.execute(outlier_sql):
        cases.append({
            "case_type": "PRICE_OUTLIER",
            "severity": "MEDIUM",
            "part_id": row["PART_ID"],
            "payload": dict(row),
        })
    # contradictory quantity signals in reconstructed package vs canonical BOM expected
    contradictory_sql = """
    SELECT npc.PACKAGE_ID, npc.ASSEMBLY_ID, npc.COMPONENT_MATCH_RATE, npc.SUPPLY_MATCH_RATE
    FROM NER_PACKAGE_COVERAGE npc
    WHERE npc.NER_RUN_ID = ? AND (npc.COMPONENT_MATCH_RATE < 0.85 OR npc.SUPPLY_MATCH_RATE < 0.85)
    LIMIT 30
    """
    for row in conn.execute(contradictory_sql, (ner_run_id,)):
        cases.append({
            "case_type": "LOW_PACKAGE_COVERAGE",
            "severity": "MEDIUM",
            "assembly_id": row["ASSEMBLY_ID"],
            "payload": dict(row),
        })
    # ambiguous repeated mention text resolving to multiple entities across runs/docs
    ambiguity_sql = """
    SELECT nm.MENTION_TEXT, COUNT(DISTINCT COALESCE(nr.PART_ID, -1) || '|' || COALESCE(nr.SUPPLY_ID, -1) || '|' || COALESCE(nr.SUPPLIER_ID, -1) || '|' || COALESCE(nr.ASSEMBLY_ID, -1)) AS ENTITY_VARIANT_COUNT
    FROM NER_MENTION nm
    JOIN NER_RESOLUTION nr ON nr.NER_MENTION_ID = nm.NER_MENTION_ID
    WHERE nm.NER_RUN_ID = ?
    GROUP BY nm.MENTION_TEXT
    HAVING ENTITY_VARIANT_COUNT > 1
    LIMIT 20
    """
    for row in conn.execute(ambiguity_sql, (ner_run_id,)):
        cases.append({
            "case_type": "AMBIGUOUS_MERGE",
            "severity": "HIGH",
            "payload": dict(row),
        })
    return cases


def make_audit_prompt(case: dict[str, Any]) -> str:
    payload = json.dumps(case["payload"], indent=2)
    return render_prompt(
        "judge/audit_case.prompt.txt",
        """
        You are a data quality auditor reviewing BOM-grounded NER output.
        Analyze this case and answer ONLY in JSON with keys:
        verdict, risk_level, finding_code, summary, recommendation, likely_wrong_merge, weak_provenance, wrong_price_mapping, contradictory_quantity, hallucinated_vs_inferred.

        CASE_TYPE: $case_type
        SEVERITY: $severity
        PAYLOAD:
        $payload
        """,
        case_type=case["case_type"],
        severity=case["severity"],
        payload=payload,
    )


def run_auditor(conn: sqlite3.Connection, cfg: dict[str, Any], root: Path) -> dict[str, Any]:
    ner_run_id = get_latest_ner_run_id(conn)
    model_name = cfg.get("auditor_model")
    if not model_name:
        raise ValueError("auditor_model must be configured; local auditor is required in this release")
    client = OllamaClient(
        cfg.get("ollama_url", "http://localhost:11434"),
        timeout_sec=int(cfg.get("auditor_timeout_sec", cfg.get("ollama_timeout_sec", 1800))),
        retry_count=int(cfg.get("auditor_retry_count", 1)),
        retry_backoff_sec=int(cfg.get("auditor_retry_backoff_sec", 10)),
    )
    audit_run_label = f"AUDIT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    conn.execute(
        "INSERT INTO AUDIT_RUN (RUN_LABEL, NER_RUN_ID, MODEL_NAME, CREATED_AT, CONFIG_JSON) VALUES (?, ?, ?, ?, ?)",
        (audit_run_label, ner_run_id, model_name, UTC_NOW(), json.dumps(cfg)),
    )
    audit_run_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    cases = build_audit_cases(conn, ner_run_id)
    case_limit = int(cfg.get("auditor_case_limit", 0) or 0)
    if case_limit > 0:
        cases = cases[:case_limit]
    log_event(f"AUDIT: starting {len(cases)} case(s) with model {model_name}")
    completed = 0
    failed = 0
    for idx, case in enumerate(cases, start=1):
        log_event(f"AUDIT: case {idx}/{len(cases)} | type={case['case_type']} | severity={case['severity']}")
        prompt = make_audit_prompt(case)
        conn.execute(
            "INSERT INTO AUDIT_CASE (AUDIT_RUN_ID, CASE_TYPE, SEVERITY, STATUS_CODE, GENERATED_DOCUMENT_ID, PART_ID, SUPPLY_ID, ASSEMBLY_ID, SOURCE_RESOLUTION_ID, PROMPT_TEXT, CREATED_AT) VALUES (?, ?, ?, 'PENDING', ?, ?, ?, ?, ?, ?, ?)",
            (audit_run_id, case["case_type"], case["severity"], case.get("generated_document_id"), case.get("part_id"), case.get("supply_id"), case.get("assembly_id"), case.get("source_resolution_id"), prompt, UTC_NOW()),
        )
        audit_case_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        try:
            response_text = client.generate(model_name, prompt, temperature=0.1, keep_alive=cfg.get("auditor_keep_alive", cfg.get("keep_alive", "30m")))
            log_llm_call(conn, 'AUDIT_CASE', 'AUDITOR', model_name, run_context=audit_run_label, generated_document_id=case.get('generated_document_id'), audit_case_id=audit_case_id, prompt_text=prompt, response_text=response_text, success_flag=True)
            parsed = safe_extract_json_block(response_text) or {
                "verdict": "MANUAL_REVIEW",
                "risk_level": case["severity"],
                "finding_code": "UNPARSEABLE_AUDIT_RESPONSE",
                "summary": response_text[:500],
                "recommendation": "Review response manually.",
            }
            conn.execute("UPDATE AUDIT_CASE SET STATUS_CODE = 'COMPLETED', RESPONSE_TEXT = ? WHERE AUDIT_CASE_ID = ?", (response_text, audit_case_id))
            conn.execute(
                "INSERT INTO AUDIT_FINDING (AUDIT_CASE_ID, FINDING_CODE, RISK_LEVEL, SUMMARY_TEXT, RECOMMENDATION_TEXT, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    audit_case_id,
                    _normalize_scalar(parsed.get("finding_code"), "NO_CODE"),
                    _normalize_scalar(parsed.get("risk_level"), case["severity"]),
                    _normalize_scalar(parsed.get("summary"), "No summary"),
                    _normalize_scalar(parsed.get("recommendation")),
                    UTC_NOW(),
                ),
            )
            completed += 1
        except Exception as exc:
            failed += 1
            err = str(exc)
            log_llm_call(conn, 'AUDIT_CASE', 'AUDITOR', model_name, run_context=audit_run_label, generated_document_id=case.get('generated_document_id'), audit_case_id=audit_case_id, prompt_text=prompt, response_text=err, success_flag=False)
            conn.execute("UPDATE AUDIT_CASE SET STATUS_CODE = 'FAILED', RESPONSE_TEXT = ? WHERE AUDIT_CASE_ID = ?", (err, audit_case_id))
            conn.execute(
                "INSERT INTO AUDIT_FINDING (AUDIT_CASE_ID, FINDING_CODE, RISK_LEVEL, SUMMARY_TEXT, RECOMMENDATION_TEXT, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)",
                (audit_case_id, 'AUDITOR_TIMEOUT_OR_ERROR', case['severity'], err[:500], 'Retry with a longer auditor timeout or a faster judge model.', UTC_NOW()),
            )
            log_event(f"AUDIT: case {idx}/{len(cases)} failed: {err}")
        if idx % max(1, int(cfg.get("audit_commit_every", 1))) == 0:
            conn.commit()
    conn.commit()
    return {
        "run_label": audit_run_label,
        "audit_run_id": audit_run_id,
        "cases_created": len(cases),
        "cases_completed": completed,
        "cases_failed": failed,
        "model_name": model_name,
        "auditor_llm_call_count": completed + failed,
    }

# ---------------------------
# DQ HTML report
# ---------------------------

def _best_provenance(values: list[str]) -> str:
    order = {'STRONG': 3, 'MEDIUM': 2, 'WEAK': 1}
    if not values:
        return ''
    return sorted(values, key=lambda v: order.get(v or '', 0), reverse=True)[0]


def build_truth_vs_discovered(conn: sqlite3.Connection, ner_run_id: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    generated_run_id = conn.execute('SELECT GENERATED_RUN_ID FROM NER_RUN WHERE NER_RUN_ID = ?', (ner_run_id,)).fetchone()[0]
    summary = []
    category_specs = [
        ('PART', 'PART_ID', 'PART', 'CANONICAL_PART_ID', 'PART_NUMBER', 'CANONICAL_NAME'),
        ('SUPPLY', 'SUPPLY_ID', 'SUPPLY', 'CANONICAL_SUPPLY_ID', 'SUPPLY_CODE', 'CANONICAL_NAME'),
        ('SUPPLIER', 'SUPPLIER_ID', 'SUPPLIER', 'SUPPLIER_CODE', 'SUPPLIER_CODE', 'CANONICAL_NAME'),
        ('ASSEMBLY', 'ASSEMBLY_ID', 'ASSEMBLY', 'ASSEMBLY_CODE', 'ASSEMBLY_CODE', 'ASSEMBLY_CODE'),
    ]
    details = []
    for label, id_col, table_name, canonical_col, code_col, name_col in category_specs:
        truth_rows = [dict(r) for r in conn.execute(f"SELECT DISTINCT gde.{id_col} AS ENTITY_ID FROM GENERATED_DOCUMENT_ENTITY gde JOIN GENERATED_DOCUMENT gd ON gd.GENERATED_DOCUMENT_ID = gde.GENERATED_DOCUMENT_ID WHERE gd.GENERATED_RUN_ID = ? AND gde.ENTITY_KIND = ? AND gde.{id_col} IS NOT NULL", (generated_run_id, label))]
        truth_ids = {r['ENTITY_ID'] for r in truth_rows}
        if table_name == 'ASSEMBLY':
            discovered_ids = {r[0] for r in conn.execute("SELECT DISTINCT nr.ASSEMBLY_ID FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ? AND nr.ASSEMBLY_ID IS NOT NULL", (ner_run_id,))}
        else:
            discovered_ids = {r[0] for r in conn.execute(f"SELECT DISTINCT nr.{id_col} FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ? AND nr.{id_col} IS NOT NULL", (ner_run_id,))}
        matched = truth_ids & discovered_ids
        false_pos = discovered_ids - truth_ids
        missed = truth_ids - discovered_ids
        summary.append({
            'category': label,
            'truth_in_docs': len(truth_ids),
            'discovered': len(discovered_ids),
            'matched': len(matched),
            'missed_truth': len(missed),
            'false_positive': len(false_pos),
            'match_rate': round(len(matched) / max(1, len(truth_ids)), 4),
        })
        for entity_id in sorted(truth_ids | discovered_ids):
            if table_name == 'ASSEMBLY':
                row = conn.execute("SELECT a.ASSEMBLY_ID, a.ASSEMBLY_CODE, a.ASSEMBLY_CODE AS DISPLAY_CODE, p.CANONICAL_NAME FROM ASSEMBLY a JOIN PART p ON p.PART_ID = a.PART_ID WHERE a.ASSEMBLY_ID = ?", (entity_id,)).fetchone()
                canonical_id = row['ASSEMBLY_CODE'] if row else str(entity_id)
                display_code = row['DISPLAY_CODE'] if row else ''
                display_name = row['CANONICAL_NAME'] if row else ''
                res_rows = [dict(r) for r in conn.execute("SELECT nr.RESOLUTION_METHOD, nr.CONFIDENCE_SCORE, nr.PROVENANCE_STRENGTH FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ? AND nr.ASSEMBLY_ID = ?", (ner_run_id, entity_id))]
            else:
                row = conn.execute(f"SELECT {canonical_col} AS CANONICAL_ID, {code_col} AS DISPLAY_CODE, {name_col} AS DISPLAY_NAME FROM {table_name} WHERE {id_col} = ?", (entity_id,)).fetchone()
                canonical_id = row['CANONICAL_ID'] if row else str(entity_id)
                display_code = row['DISPLAY_CODE'] if row else ''
                display_name = row['DISPLAY_NAME'] if row else ''
                res_rows = [dict(r) for r in conn.execute(f"SELECT nr.RESOLUTION_METHOD, nr.CONFIDENCE_SCORE, nr.PROVENANCE_STRENGTH FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ? AND nr.{id_col} = ?", (ner_run_id, entity_id))]
            methods = sorted({r['RESOLUTION_METHOD'] for r in res_rows})
            max_conf = max([r['CONFIDENCE_SCORE'] for r in res_rows], default=None)
            best_prov = _best_provenance([r['PROVENANCE_STRENGTH'] for r in res_rows])
            details.append({
                'category': label,
                'canonical_id': canonical_id,
                'display_code': display_code,
                'display_name': display_name,
                'truth_in_docs': entity_id in truth_ids,
                'discovered': entity_id in discovered_ids,
                'methods': ', '.join(methods),
                'confidence': None if max_conf is None else round(max_conf, 3),
                'provenance': best_prov,
            })
    # examples
    correct_candidates = []
    problematic_candidates = []
    for row in conn.execute("SELECT nm.NER_MENTION_ID, gd.DOC_ID, dt.TYPE_CODE, nm.MENTION_TEXT, nr.RESOLVED_ENTITY_KIND, nr.RESOLUTION_METHOD, nr.CONFIDENCE_SCORE, nr.PROVENANCE_STRENGTH, nr.PART_ID, nr.SUPPLY_ID, nr.SUPPLIER_ID, nr.ASSEMBLY_ID, gd.GENERATED_DOCUMENT_ID FROM NER_MENTION nm LEFT JOIN NER_RESOLUTION nr ON nr.NER_MENTION_ID = nm.NER_MENTION_ID JOIN GENERATED_DOCUMENT gd ON gd.GENERATED_DOCUMENT_ID = nm.GENERATED_DOCUMENT_ID JOIN DOCUMENT_TYPE dt ON dt.DOCUMENT_TYPE_ID = gd.DOCUMENT_TYPE_ID WHERE nm.NER_RUN_ID = ?", (ner_run_id,)):
        row = dict(row)
        resolved = row['RESOLVED_ENTITY_KIND'] is not None
        entity_kind = row.get('RESOLVED_ENTITY_KIND')
        correct = False
        if resolved:
            if entity_kind == 'PART' and row.get('PART_ID') is not None:
                correct = conn.execute("SELECT COUNT(*) FROM GENERATED_DOCUMENT_ENTITY WHERE GENERATED_DOCUMENT_ID = ? AND ENTITY_KIND = 'PART' AND PART_ID = ?", (row['GENERATED_DOCUMENT_ID'], row['PART_ID'])).fetchone()[0] > 0
            elif entity_kind == 'SUPPLY' and row.get('SUPPLY_ID') is not None:
                correct = conn.execute("SELECT COUNT(*) FROM GENERATED_DOCUMENT_ENTITY WHERE GENERATED_DOCUMENT_ID = ? AND ENTITY_KIND = 'SUPPLY' AND SUPPLY_ID = ?", (row['GENERATED_DOCUMENT_ID'], row['SUPPLY_ID'])).fetchone()[0] > 0
            elif entity_kind == 'SUPPLIER' and row.get('SUPPLIER_ID') is not None:
                correct = conn.execute("SELECT COUNT(*) FROM GENERATED_DOCUMENT_ENTITY WHERE GENERATED_DOCUMENT_ID = ? AND ENTITY_KIND = 'SUPPLIER' AND SUPPLIER_ID = ?", (row['GENERATED_DOCUMENT_ID'], row['SUPPLIER_ID'])).fetchone()[0] > 0
            elif entity_kind == 'ASSEMBLY' and row.get('ASSEMBLY_ID') is not None:
                correct = conn.execute("SELECT COUNT(*) FROM GENERATED_DOCUMENT_ENTITY WHERE GENERATED_DOCUMENT_ID = ? AND ENTITY_KIND = 'ASSEMBLY' AND ASSEMBLY_ID = ?", (row['GENERATED_DOCUMENT_ID'], row['ASSEMBLY_ID'])).fetchone()[0] > 0
        example = {
            'doc_id': row['DOC_ID'],
            'doc_type': row['TYPE_CODE'],
            'mention_text': row['MENTION_TEXT'],
            'resolved_kind': row.get('RESOLVED_ENTITY_KIND') or 'UNRESOLVED',
            'method': row.get('RESOLUTION_METHOD') or 'NONE',
            'confidence': row.get('CONFIDENCE_SCORE'),
            'provenance': row.get('PROVENANCE_STRENGTH') or '',
            'outcome': 'CORRECT' if correct else ('UNRESOLVED' if not resolved else 'PROBLEMATIC'),
        }
        if correct and row.get('CONFIDENCE_SCORE', 0) >= 0.85 and row.get('PROVENANCE_STRENGTH') != 'WEAK':
            correct_candidates.append(example)
        elif not correct or not resolved or row.get('PROVENANCE_STRENGTH') == 'WEAK':
            problematic_candidates.append(example)
    rng = random.Random(ner_run_id)
    rng.shuffle(correct_candidates)
    rng.shuffle(problematic_candidates)
    examples = correct_candidates[:2] + problematic_candidates[:1]
    return summary, details, examples

DARK_CSS = """
:root {
  --bg: #0d1117;
  --panel: #161b22;
  --panel-2: #1f2630;
  --text: #e6edf3;
  --muted: #9fb0c3;
  --accent: #74c0fc;
  --accent-2: #ffd166;
  --ok: #2ecc71;
  --warn: #f4d35e;
  --bad: #ff6b6b;
  --grid: #2d3643;
}
body { background: var(--bg); color: var(--text); font-family: Inter, Segoe UI, Arial, sans-serif; margin: 0; }
.container { max-width: 1180px; margin: 0 auto; padding: 28px; }
h1, h2, h3 { margin: 0 0 12px; }
p { color: var(--muted); }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin: 18px 0 26px; }
.card { background: linear-gradient(180deg, var(--panel), var(--panel-2)); border: 1px solid var(--grid); border-radius: 16px; padding: 18px; box-shadow: 0 10px 28px rgba(0,0,0,.22); }
.metric { font-size: 34px; font-weight: 700; color: var(--accent); }
.label { font-size: 12px; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); }
.table { width: 100%; border-collapse: collapse; margin-top: 12px; }
.table th, .table td { border-bottom: 1px solid var(--grid); padding: 10px 8px; text-align: left; vertical-align: top; }
.table th { color: var(--accent-2); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
.badge { padding: 4px 8px; border-radius: 999px; font-size: 12px; display: inline-block; }
.badge.ok { background: rgba(46,204,113,.12); color: var(--ok); }
.badge.warn { background: rgba(244,211,94,.12); color: var(--warn); }
.badge.bad { background: rgba(255,107,107,.12); color: var(--bad); }
.small { font-size: 13px; color: var(--muted); }
.code { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; color: var(--accent); }
"""


def dq_metric(conn: sqlite3.Connection, dq_run_id: int, code: str, value: float, unit: str | None = None, context: str | None = None) -> None:
    conn.execute(
        "INSERT INTO DQ_METRIC (DQ_RUN_ID, METRIC_CODE, METRIC_VALUE, METRIC_UNIT, METRIC_CONTEXT) VALUES (?, ?, ?, ?, ?)",
        (dq_run_id, code, value, unit, context),
    )


def run_dq_report(conn: sqlite3.Connection, root: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    ner_run = conn.execute("SELECT * FROM NER_RUN ORDER BY CREATED_AT DESC LIMIT 1").fetchone()
    if not ner_run:
        raise ValueError("No NER run found for DQ report")
    audit_run = conn.execute("SELECT * FROM AUDIT_RUN WHERE NER_RUN_ID = ? ORDER BY CREATED_AT DESC LIMIT 1", (ner_run["NER_RUN_ID"],)).fetchone()

    report_dir = ensure_dir(root / "reports")
    report_name = f"dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    report_path = report_dir / report_name
    dq_run_label = report_name.replace('.html', '')
    conn.execute(
        "INSERT INTO DQ_RUN (RUN_LABEL, NER_RUN_ID, AUDIT_RUN_ID, REPORT_HTML_PATH, CREATED_AT) VALUES (?, ?, ?, ?, ?)",
        (dq_run_label, ner_run["NER_RUN_ID"], audit_run["AUDIT_RUN_ID"] if audit_run else None, str(report_path), UTC_NOW()),
    )
    dq_run_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    total_mentions = conn.execute("SELECT COUNT(*) FROM NER_MENTION WHERE NER_RUN_ID = ?", (ner_run["NER_RUN_ID"],)).fetchone()[0]
    resolved_mentions = conn.execute("SELECT COUNT(*) FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ?", (ner_run["NER_RUN_ID"],)).fetchone()[0]
    unresolved = total_mentions - resolved_mentions
    exactish = conn.execute("SELECT COUNT(*) FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ? AND nr.RESOLUTION_METHOD IN ('DIRECT_PART_NUMBER', 'DIRECT_SUPPLY_CODE', 'ASSEMBLY_CODE', 'ASSEMBLY_PART_NUMBER', 'SUPPLIER_CODE', 'CANONICAL_NAME', 'ASSEMBLY_NAME')", (ner_run["NER_RUN_ID"],)).fetchone()[0]
    weak = conn.execute("SELECT COUNT(*) FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ? AND nr.PROVENANCE_STRENGTH = 'WEAK'", (ner_run["NER_RUN_ID"],)).fetchone()[0]
    conflicts = conn.execute("SELECT COUNT(*) FROM NER_CONFLICT WHERE NER_RUN_ID = ?", (ner_run["NER_RUN_ID"],)).fetchone()[0]
    comp_avg, sup_avg = conn.execute("SELECT AVG(COMPONENT_MATCH_RATE), AVG(SUPPLY_MATCH_RATE) FROM NER_PACKAGE_COVERAGE WHERE NER_RUN_ID = ?", (ner_run["NER_RUN_ID"],)).fetchone()
    auditor_findings = conn.execute("SELECT COUNT(*) FROM AUDIT_FINDING af JOIN AUDIT_CASE ac ON ac.AUDIT_CASE_ID = af.AUDIT_CASE_ID WHERE ac.AUDIT_RUN_ID = ?", (audit_run["AUDIT_RUN_ID"],)).fetchone()[0] if audit_run else 0

    metrics = {
        "TOTAL_MENTION": total_mentions,
        "RESOLVED_RATE": round(resolved_mentions / max(1, total_mentions), 4),
        "UNRESOLVED_RATE": round(unresolved / max(1, total_mentions), 4),
        "EXACTISH_RATE": round(exactish / max(1, resolved_mentions), 4),
        "WEAK_PROVENANCE_RATE": round(weak / max(1, resolved_mentions), 4),
        "CONFLICT_COUNT": conflicts,
        "COMPONENT_MATCH_RATE_AVG": round(comp_avg or 0.0, 4),
        "SUPPLY_MATCH_RATE_AVG": round(sup_avg or 0.0, 4),
        "AUDITOR_FINDING_COUNT": auditor_findings,
    }
    for code, value in metrics.items():
        dq_metric(conn, dq_run_id, code, float(value))

    findings = []
    if metrics["UNRESOLVED_RATE"] > 0.2:
        findings.append(("UNRESOLVED_GAP", "BAD", "Too many mentions remain unresolved", "Tighten extraction noise and push more weak cases through resolver review."))
    if metrics["EXACTISH_RATE"] < 0.45:
        findings.append(("LOW_EXACT_EVIDENCE", "BAD", "Exact-code evidence is low", "Increase part-number and supply-code coverage in docs or reduce reliance on fuzzy fallback."))
    if metrics["COMPONENT_MATCH_RATE_AVG"] < 0.9 or metrics["SUPPLY_MATCH_RATE_AVG"] < 0.9:
        findings.append(("PACKAGE_RECONSTRUCTION_GAP", "BAD", "Package reconstruction is weaker than target", "Inspect low-coverage packages against canonical truth."))
    if auditor_findings > 0:
        findings.append(("AUDITOR_ACTIVE", "WARN", "Auditor identified flagged cases", "Review the latest audit findings before presenting the run publicly."))
    for ftype, sev, summary, detail in findings:
        conn.execute("INSERT INTO DQ_FINDING (DQ_RUN_ID, FINDING_TYPE, SEVERITY, SUMMARY_TEXT, DETAIL_TEXT) VALUES (?, ?, ?, ?, ?)", (dq_run_id, ftype, sev, summary, detail))

    tvd_summary, tvd_details, ner_examples = build_truth_vs_discovered(conn, ner_run["NER_RUN_ID"])

    top_methods = conn.execute("SELECT nr.RESOLUTION_METHOD, COUNT(*) AS CNT FROM NER_RESOLUTION nr JOIN NER_MENTION nm ON nm.NER_MENTION_ID = nr.NER_MENTION_ID WHERE nm.NER_RUN_ID = ? GROUP BY nr.RESOLUTION_METHOD ORDER BY CNT DESC LIMIT 10", (ner_run["NER_RUN_ID"],)).fetchall()
    low_packages = conn.execute("SELECT PACKAGE_ID, COMPONENT_MATCH_RATE, SUPPLY_MATCH_RATE FROM NER_PACKAGE_COVERAGE WHERE NER_RUN_ID = ? ORDER BY COMPONENT_MATCH_RATE ASC, SUPPLY_MATCH_RATE ASC LIMIT 10", (ner_run["NER_RUN_ID"],)).fetchall()
    audit_table = conn.execute("SELECT ac.CASE_TYPE, af.RISK_LEVEL, af.SUMMARY_TEXT, af.RECOMMENDATION_TEXT FROM AUDIT_FINDING af JOIN AUDIT_CASE ac ON ac.AUDIT_CASE_ID = af.AUDIT_CASE_ID WHERE ac.AUDIT_RUN_ID = ? ORDER BY af.AUDIT_FINDING_ID DESC LIMIT 12", (audit_run["AUDIT_RUN_ID"],)).fetchall() if audit_run else []

    def sev_badge(sev: str) -> str:
        cls = "ok" if sev == "OK" else "warn" if sev in {"WARN", "MEDIUM"} else "bad"
        return f'<span class="badge {cls}">{html.escape(sev)}</span>'

    html_doc = [
        "<!doctype html><html><head><meta charset='utf-8'><title>Europa NER DQ Report</title>",
        f"<style>{DARK_CSS}</style></head><body><div class='container'>",
        "<h1>Europa NER Truth, Discovery & Trust Report</h1>",
        f"<p>NER run: <span class='code'>{html.escape(ner_run['RUN_LABEL'])}</span> &nbsp; Auditor run: <span class='code'>{html.escape(audit_run['RUN_LABEL']) if audit_run else 'none'}</span></p>",
        "<div class='grid'>",
    ]
    cards = [
        ("Resolved rate", f"{metrics['RESOLVED_RATE']*100:.1f}%"),
        ("Weak provenance", f"{metrics['WEAK_PROVENANCE_RATE']*100:.1f}%"),
        ("Component match avg", f"{metrics['COMPONENT_MATCH_RATE_AVG']*100:.1f}%"),
        ("Supply match avg", f"{metrics['SUPPLY_MATCH_RATE_AVG']*100:.1f}%"),
        ("Auditor findings", str(auditor_findings)),
        ("Conflict count", str(conflicts)),
    ]
    for label, value in cards:
        html_doc.append(f"<div class='card'><div class='label'>{html.escape(label)}</div><div class='metric'>{html.escape(value)}</div></div>")
    html_doc.append("</div>")

    html_doc.append("<div class='card'><h2>Executive readout</h2><table class='table'><thead><tr><th>Finding</th><th>Severity</th><th>Detail</th></tr></thead><tbody>")
    for ftype, sev, summary, detail in findings or [("BASELINE", "OK", "No major DQ rules fired", "Continue with harder synthetic corruption to stress the trust story.")]:
        html_doc.append(f"<tr><td>{html.escape(summary)}</td><td>{sev_badge(sev)}</td><td>{html.escape(detail)}</td></tr>")
    html_doc.append("</tbody></table></div>")

    html_doc.append("<div class='card'><h2>Truth vs discovered by category</h2><table class='table'><thead><tr><th>Category</th><th>Truth in docs</th><th>Discovered</th><th>Matched</th><th>Missed truth</th><th>False positive</th><th>Match rate</th></tr></thead><tbody>")
    for row in tvd_summary:
        html_doc.append(f"<tr><td>{html.escape(row['category'])}</td><td>{row['truth_in_docs']}</td><td>{row['discovered']}</td><td>{row['matched']}</td><td>{row['missed_truth']}</td><td>{row['false_positive']}</td><td>{row['match_rate']:.3f}</td></tr>")
    html_doc.append("</tbody></table></div>")

    html_doc.append("<div class='card'><h2>Truth vs discovered detail</h2><table class='table'><thead><tr><th>Category</th><th>Canonical ID</th><th>Code</th><th>Name</th><th>Truth in docs</th><th>Discovered</th><th>Method</th><th>Confidence</th><th>Provenance</th></tr></thead><tbody>")
    for row in tvd_details[:80]:
        conf = '' if row['confidence'] is None else f"{row['confidence']:.3f}"
        html_doc.append(f"<tr><td>{html.escape(row['category'])}</td><td>{html.escape(str(row['canonical_id']))}</td><td>{html.escape(str(row['display_code']))}</td><td>{html.escape(str(row['display_name']))}</td><td>{'Y' if row['truth_in_docs'] else 'N'}</td><td>{'Y' if row['discovered'] else 'N'}</td><td>{html.escape(row['methods'])}</td><td>{conf}</td><td>{html.escape(row['provenance'])}</td></tr>")
    html_doc.append("</tbody></table><p class='small'>Showing first 80 rows from the truth-vs-discovered detail set.</p></div>")

    html_doc.append("<div class='card'><h2>NER examples: 2 correct + 1 problematic</h2><table class='table'><thead><tr><th>Doc</th><th>Type</th><th>Mention</th><th>Outcome</th><th>Method</th><th>Confidence</th><th>Provenance</th></tr></thead><tbody>")
    for ex in ner_examples:
        conf = '' if ex['confidence'] is None else f"{ex['confidence']:.3f}"
        html_doc.append(f"<tr><td>{html.escape(ex['doc_id'])}</td><td>{html.escape(ex['doc_type'])}</td><td>{html.escape(ex['mention_text'])}</td><td>{html.escape(ex['outcome'])}</td><td>{html.escape(ex['method'])}</td><td>{conf}</td><td>{html.escape(ex['provenance'])}</td></tr>")
    if not ner_examples:
        html_doc.append("<tr><td colspan='7' class='small'>No examples available.</td></tr>")
    html_doc.append("</tbody></table></div>")

    html_doc.append("<div class='card'><h2>Resolution method distribution</h2><table class='table'><thead><tr><th>Method</th><th>Count</th></tr></thead><tbody>")
    for row in top_methods:
        html_doc.append(f"<tr><td>{html.escape(row['RESOLUTION_METHOD'])}</td><td>{row['CNT']}</td></tr>")
    html_doc.append("</tbody></table></div>")

    html_doc.append("<div class='card'><h2>Lowest-coverage packages</h2><table class='table'><thead><tr><th>Package</th><th>Component match</th><th>Supply match</th></tr></thead><tbody>")
    for row in low_packages:
        html_doc.append(f"<tr><td>{html.escape(row['PACKAGE_ID'])}</td><td>{row['COMPONENT_MATCH_RATE']:.3f}</td><td>{row['SUPPLY_MATCH_RATE']:.3f}</td></tr>")
    html_doc.append("</tbody></table></div>")

    html_doc.append("<div class='card'><h2>Auditor/Judge findings</h2><table class='table'><thead><tr><th>Case type</th><th>Risk</th><th>Summary</th><th>Recommendation</th></tr></thead><tbody>")
    if audit_table:
        for row in audit_table:
            html_doc.append(f"<tr><td>{html.escape(row['CASE_TYPE'])}</td><td>{sev_badge(row['RISK_LEVEL'])}</td><td>{html.escape(row['SUMMARY_TEXT'])}</td><td>{html.escape(row['RECOMMENDATION_TEXT'] or '')}</td></tr>")
    else:
        html_doc.append("<tr><td colspan='4' class='small'>No auditor findings available.</td></tr>")
    html_doc.append("</tbody></table></div>")

    html_doc.append("</div></body></html>")
    report_path.write_text("".join(html_doc), encoding="utf-8")
    conn.commit()
    return {
        "dq_run_label": dq_run_label,
        "report_html_path": str(report_path),
        "metrics": metrics,
        "finding_count": len(findings),
        "truth_vs_discovered_categories": len(tvd_summary),
        "ner_example_count": len(ner_examples),
    }


# ---------------------------
# ERD asset
# ---------------------------

MERMAID_ERD = """
erDiagram
    UNIT ||--o{ PART : "UNIT_ID"
    UNIT ||--o{ SUPPLY : "UNIT_ID"
    UNIT ||--o{ PART_ATTRIBUTE : "UNIT_ID"
    UNIT ||--o{ SUPPLY_ATTRIBUTE : "UNIT_ID"
    UNIT ||--o{ ASSEMBLY_BOM_LINE : "UNIT_ID"
    UNIT ||--o{ ASSEMBLY_SUPPLY_REQUIREMENT : "UNIT_ID"
    UNIT ||--o{ ATTRIBUTE_DEFINITION : "DEFAULT_UNIT_ID"

    LIFECYCLE_STATUS ||--o{ PART : "LIFECYCLE_STATUS_ID"
    LIFECYCLE_STATUS ||--o{ SUPPLY : "LIFECYCLE_STATUS_ID"
    PART_TYPE ||--o{ PART : "PART_TYPE_ID"
    PART_CATEGORY ||--o{ PART : "PART_CATEGORY_ID"
    SUPPLY_CATEGORY ||--o{ SUPPLY : "SUPPLY_CATEGORY_ID"
    DOCUMENT_TYPE ||--o{ GENERATED_DOCUMENT : "DOCUMENT_TYPE_ID"

    PART ||--|| ASSEMBLY : "PART_ID"
    PART ||--o{ PART_ALIAS : "PART_ID"
    PART ||--o{ PART_ATTRIBUTE : "PART_ID"
    PART ||--o{ SUPPLIER_PART : "PART_ID"
    PART ||--o{ ASSEMBLY_BOM_LINE : "CHILD_PART_ID"
    ASSEMBLY ||--o{ ASSEMBLY_BOM_LINE : "ASSEMBLY_ID"
    SUPPLY ||--o{ SUPPLY_ALIAS : "SUPPLY_ID"
    SUPPLY ||--o{ SUPPLY_ATTRIBUTE : "SUPPLY_ID"
    SUPPLY ||--o{ SUPPLIER_SUPPLY : "SUPPLY_ID"
    ASSEMBLY ||--o{ ASSEMBLY_SUPPLY_REQUIREMENT : "ASSEMBLY_ID"
    SUPPLY ||--o{ ASSEMBLY_SUPPLY_REQUIREMENT : "SUPPLY_ID"

    SUPPLIER ||--o{ SUPPLIER_ALIAS : "SUPPLIER_ID"
    SUPPLIER ||--o{ SUPPLIER_PART : "SUPPLIER_ID"
    SUPPLIER ||--o{ SUPPLIER_SUPPLY : "SUPPLIER_ID"

    GENERATED_RUN ||--o{ GENERATED_DOCUMENT : "GENERATED_RUN_ID"
    GENERATED_DOCUMENT ||--o{ GENERATED_DOCUMENT_ENTITY : "GENERATED_DOCUMENT_ID"
    ASSEMBLY ||--o{ GENERATED_DOCUMENT : "ASSEMBLY_ID"

    NER_RUN ||--o{ NER_MENTION : "NER_RUN_ID"
    NER_MENTION ||--|| NER_RESOLUTION : "NER_MENTION_ID"
    NER_RUN ||--o{ NER_CONFLICT : "NER_RUN_ID"
    NER_RUN ||--o{ NER_PACKAGE_COVERAGE : "NER_RUN_ID"
    NER_RUN ||--o{ NER_RECONSTRUCTED_BOM_LINE : "NER_RUN_ID"
    NER_RUN ||--o{ NER_RECONSTRUCTED_SUPPLY_REQUIREMENT : "NER_RUN_ID"

    AUDIT_RUN ||--o{ AUDIT_CASE : "AUDIT_RUN_ID"
    AUDIT_CASE ||--o{ AUDIT_FINDING : "AUDIT_CASE_ID"
    NER_RUN ||--o{ AUDIT_RUN : "NER_RUN_ID"

    DQ_RUN ||--o{ DQ_METRIC : "DQ_RUN_ID"
    DQ_RUN ||--o{ DQ_FINDING : "DQ_RUN_ID"
    NER_RUN ||--o{ DQ_RUN : "NER_RUN_ID"
    AUDIT_RUN ||--o{ DQ_RUN : "AUDIT_RUN_ID"
""".strip() + "\n"


def write_erd_asset(root: Path) -> Path:
    docs = ensure_dir(root / "assets")
    path = docs / "ERD_MERMAID.md"
    path.write_text("```mermaid\n" + MERMAID_ERD + "```\n", encoding="utf-8")
    return path

# ---------------------------
# CLI
# ---------------------------


def init_workspace(root: Path, reset: bool) -> Path:
    if reset and root.exists():
        import shutil
        shutil.rmtree(root)
    ensure_dir(root)
    ensure_dir(root / "db")
    ensure_dir(root / "docs")
    ensure_dir(root / "exports")
    ensure_dir(root / "manifests")
    ensure_dir(root / "reports")
    ensure_dir(root / "sql")
    ensure_dir(root / "assets")
    ensure_dir(root / "logs")
    db_path = root / "db" / "europa_masterdata.sqlite"
    conn = open_db(db_path)
    create_schema(conn)
    seed_lookups(conn)
    seed_attribute_definitions(conn)
    conn.close()
    write_erd_asset(root)
    return db_path



def export_audit_finetune_dataset(root: Path) -> dict[str, Any]:
    db_path = root / 'db' / 'europa_masterdata.sqlite'
    conn = open_db(db_path)
    rows = [dict(r) for r in conn.execute("""
        SELECT ac.AUDIT_CASE_ID, ac.CASE_TYPE, ac.SEVERITY, ac.PROMPT_TEXT, ac.RESPONSE_TEXT,
               af.FINDING_CODE, af.RISK_LEVEL, af.SUMMARY_TEXT, af.RECOMMENDATION_TEXT
        FROM AUDIT_CASE ac
        LEFT JOIN AUDIT_FINDING af ON af.AUDIT_CASE_ID = ac.AUDIT_CASE_ID
        ORDER BY ac.AUDIT_CASE_ID
    """)]
    out_dir = ensure_dir(root / 'finetune')
    jsonl_path = out_dir / f'auditor_train_{datetime.now().strftime("%Y%m%d_%H%M%S")}.jsonl'
    count = 0
    with jsonl_path.open('w', encoding='utf-8') as f:
        for row in rows:
            target = {
                'finding_code': row.get('FINDING_CODE') or 'NO_CODE',
                'risk_level': row.get('RISK_LEVEL') or row.get('SEVERITY') or 'MEDIUM',
                'summary': row.get('SUMMARY_TEXT') or 'No summary',
                'recommendation': row.get('RECOMMENDATION_TEXT') or 'Review manually.'
            }
            rec = {
                'messages': [
                    {'role': 'system', 'content': 'You are a data quality audit judge for entity resolution and provenance quality. Return strict JSON.'},
                    {'role': 'user', 'content': row.get('PROMPT_TEXT') or ''},
                    {'role': 'assistant', 'content': json.dumps(target, ensure_ascii=False)}
                ]
            }
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
            count += 1
    conn.close()
    return {'jsonl_path': str(jsonl_path), 'example_count': count}

def run_demo(root: Path, cfg: dict[str, Any], run_label: str, reset: bool) -> dict[str, Any]:
    db_path = init_workspace(root, reset)
    conn = open_db(db_path)
    seed_summary = seed_master_data(conn, cfg)
    save_manifest(root, "seed_summary", run_label, seed_summary)
    generation_summary = generate_documents(conn, root, cfg, run_label)
    save_manifest(root, "generation_summary", run_label, generation_summary)
    ner_summary = run_ner(conn, root, cfg, run_label)
    save_manifest(root, "ner_summary", run_label, ner_summary)
    audit_summary = run_auditor(conn, cfg, root)
    save_manifest(root, "audit_summary", run_label, audit_summary)
    dq_summary = run_dq_report(conn, root, cfg)
    save_manifest(root, "dq_summary", run_label, dq_summary)
    usage_summary = llm_usage_summary(conn)
    exported_csvs = export_core_csvs(conn, root)
    conn.close()
    summary = {
        "root": str(root),
        "db_path": str(db_path),
        "seed_summary": seed_summary,
        "generation_summary": generation_summary,
        "ner_summary": ner_summary,
        "audit_summary": audit_summary,
        "dq_summary": dq_summary,
        "llm_usage_summary": usage_summary,
        "exported_csvs": exported_csvs,
        "erd_asset": str(root / "assets" / "ERD_MERMAID.md"),
    }
    summary["manifest_path"] = str(save_manifest(root, "run_summary", run_label, summary))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone Europa identity pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_demo = sub.add_parser("demo-run")
    p_demo.add_argument("--config", required=True)
    p_demo.add_argument("--root", required=True)
    p_demo.add_argument("--run-label", required=True)
    p_demo.add_argument("--reset", action="store_true")

    p_init = sub.add_parser("init-db")
    p_init.add_argument("--root", required=True)
    p_init.add_argument("--reset", action="store_true")

    p_seed = sub.add_parser("seed")
    p_seed.add_argument("--config", required=True)
    p_seed.add_argument("--root", required=True)
    p_seed.add_argument("--reset", action="store_true")

    p_gen = sub.add_parser("generate-docs")
    p_gen.add_argument("--config", required=True)
    p_gen.add_argument("--root", required=True)
    p_gen.add_argument("--run-label", required=True)

    p_ner = sub.add_parser("run-ner")
    p_ner.add_argument("--config", required=True)
    p_ner.add_argument("--root", required=True)
    p_ner.add_argument("--docs-run", required=True)

    p_audit = sub.add_parser("run-audit")
    p_audit.add_argument("--config", required=True)
    p_audit.add_argument("--root", required=True)

    p_dq = sub.add_parser("run-dq")
    p_dq.add_argument("--config", required=True)
    p_dq.add_argument("--root", required=True)

    p_ft = sub.add_parser("export-audit-finetune")
    p_ft.add_argument("--root", required=True)

    args = parser.parse_args()
    if args.cmd == "init-db":
        configure_logging(Path(args.root), 'INIT', 'init_db')
        db_path = init_workspace(Path(args.root), args.reset)
        summary = {"root": str(Path(args.root)), "db_path": str(db_path)}
        summary["manifest_path"] = str(save_manifest(Path(args.root), "init_summary", "init_db", summary))
        print(json.dumps(summary, indent=2))
        return

    if args.cmd == "export-audit-finetune":
        summary = export_audit_finetune_dataset(Path(args.root))
        summary["manifest_path"] = str(save_manifest(Path(args.root), "finetune_export", "audit_finetune", summary))
        print(json.dumps(summary, indent=2))
        return

    cfg = load_json(Path(args.config))
    root = Path(args.root)

    if args.cmd == "seed":
        configure_logging(root, 'SEED', 'seed')
        db_path = init_workspace(root, args.reset)
        conn = open_db(db_path)
        summary = seed_master_data(conn, cfg)
        conn.close()
        payload = {"root": str(root), "db_path": str(db_path), "seed_summary": summary}
        payload["manifest_path"] = str(save_manifest(root, "seed_summary", "seed", payload))
        print(json.dumps(payload, indent=2))
        return

    if args.cmd == "generate-docs":
        configure_logging(root, 'GENERATE', args.run_label)
        db_path = init_workspace(root, False)
        conn = open_db(db_path)
        summary = generate_documents(conn, root, cfg, args.run_label)
        conn.close()
        summary["manifest_path"] = str(save_manifest(root, "generation_summary", args.run_label, summary))
        print(json.dumps(summary, indent=2))
        return

    if args.cmd == "run-ner":
        configure_logging(root, 'NER', args.docs_run)
        conn = open_db(root / "db" / "europa_masterdata.sqlite")
        summary = run_ner(conn, root, cfg, args.docs_run)
        conn.close()
        summary["manifest_path"] = str(save_manifest(root, "ner_summary", args.docs_run, summary))
        print(json.dumps(summary, indent=2))
        return

    if args.cmd == "run-audit":
        configure_logging(root, 'AUDIT', 'run_audit')
        conn = open_db(root / "db" / "europa_masterdata.sqlite")
        summary = run_auditor(conn, cfg, root)
        conn.close()
        summary["manifest_path"] = str(save_manifest(root, "audit_summary", "run_audit", summary))
        print(json.dumps(summary, indent=2))
        return

    if args.cmd == "run-dq":
        configure_logging(root, 'DQ', 'run_dq')
        conn = open_db(root / "db" / "europa_masterdata.sqlite")
        summary = run_dq_report(conn, root, cfg)
        summary["exported_csvs"] = export_core_csvs(conn, root)
        conn.close()
        summary["manifest_path"] = str(save_manifest(root, "dq_summary", "run_dq", summary))
        print(json.dumps(summary, indent=2))
        return

    if args.cmd == "demo-run":
        configure_logging(root, 'DEMO', args.run_label)
        summary = run_demo(root, cfg, args.run_label, args.reset)
        print(json.dumps(summary, indent=2))
        return

if __name__ == "__main__":
    main()
