#!/usr/bin/env python3
"""Distributed worker coordinator for Nightmare/Fozzy/Extractor.

This process runs on worker VMs and coordinates work through the central server APIs:
- claims next Nightmare target,
- heartbeats active leases,
- checkpoints crawl session state to Postgres via server APIs,
- enqueues and runs Fozzy + Extractor stage jobs,
- uploads key artifacts so any VM can resume the pipeline,
- polls fleet output_clear_generation and wipes local output/ when the operator bumps it on the central host.
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from output_cleanup import FLEET_GEN_APPLIED_FILENAME, clear_output_root_children

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH_DEFAULT = BASE_DIR / "config" / "coordinator.json"
OUTPUT_ROOT_DEFAULT = BASE_DIR / "output"


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8-sig")
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _merged_value(cli_value: Any, cfg: dict[str, Any], key: str, default: Any) -> Any:
    if cli_value is not None:
        return cli_value
    if key in cfg:
        return cfg[key]
    return default


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _domain_output_dir(root_domain: str, output_root: Path) -> Path:
    return output_root / root_domain


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass
class CoordinatorConfig:
    server_base_url: str
    api_token: str
    output_root: Path
    heartbeat_interval_seconds: float
    lease_seconds: int
    poll_interval_seconds: float
    nightmare_workers: int
    fozzy_workers: int
    extractor_workers: int
    python_executable: str
    nightmare_config: Path
    fozzy_config: Path
    extractor_config: Path
    upload_session_every_seconds: float
    enable_nightmare: bool
    enable_fozzy: bool
    enable_extractor: bool
    fozzy_process_workers: int
    extractor_process_workers: int


class CoordinatorClient:
    def __init__(self, base_url: str, token: str, timeout_seconds: float = 20.0):
        self.base_url = base_url.rstrip("/")
        self.token = token.strip()
        self.timeout_seconds = timeout_seconds

    def _headers(self) -> dict[str, str]:
        out = {"Content-Type": "application/json"}
        if self.token:
            out["Authorization"] = f"Bearer {self.token}"
        return out

    def _request_json(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        data = None
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = Request(url=url, method=method.upper(), data=data, headers=self._headers())
        try:
            with urlopen(req, timeout=self.timeout_seconds) as rsp:
                raw = rsp.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} {method} {path}: {body[:400]}") from exc
        except URLError as exc:
            raise RuntimeError(f"Network error {method} {path}: {exc}") from exc
        try:
            parsed = json.loads(raw or "{}")
        except Exception:
            parsed = {}
        return parsed if isinstance(parsed, dict) else {}

    def claim_target(self, worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
        rsp = self._request_json(
            "POST",
            "/api/coord/claim",
            {"worker_id": worker_id, "lease_seconds": int(lease_seconds)},
        )
        return rsp.get("entry") if isinstance(rsp.get("entry"), dict) else None

    def heartbeat_target(self, entry_id: str, worker_id: str, lease_seconds: int) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/heartbeat",
            {"entry_id": entry_id, "worker_id": worker_id, "lease_seconds": int(lease_seconds)},
        )
        return bool(rsp.get("ok"))

    def complete_target(self, entry_id: str, worker_id: str, exit_code: int, error: str = "") -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/complete",
            {"entry_id": entry_id, "worker_id": worker_id, "exit_code": int(exit_code), "error": str(error)},
        )
        return bool(rsp.get("ok"))

    def enqueue_stage(self, root_domain: str, stage: str) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/enqueue",
            {"root_domain": root_domain, "stage": stage},
        )
        return bool(rsp.get("ok"))

    def claim_stage(self, worker_id: str, stage: str, lease_seconds: int) -> dict[str, Any] | None:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/claim",
            {"worker_id": worker_id, "stage": stage, "lease_seconds": int(lease_seconds)},
        )
        return rsp.get("entry") if isinstance(rsp.get("entry"), dict) else None

    def heartbeat_stage(self, worker_id: str, root_domain: str, stage: str, lease_seconds: int) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/heartbeat",
            {
                "worker_id": worker_id,
                "root_domain": root_domain,
                "stage": stage,
                "lease_seconds": int(lease_seconds),
            },
        )
        return bool(rsp.get("ok"))

    def complete_stage(self, worker_id: str, root_domain: str, stage: str, exit_code: int, error: str = "") -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/stage/complete",
            {
                "worker_id": worker_id,
                "root_domain": root_domain,
                "stage": stage,
                "exit_code": int(exit_code),
                "error": str(error),
            },
        )
        return bool(rsp.get("ok"))

    def load_session(self, root_domain: str) -> dict[str, Any] | None:
        query = urlencode({"root_domain": root_domain})
        rsp = self._request_json("GET", f"/api/coord/session?{query}")
        if not bool(rsp.get("found")):
            return None
        session = rsp.get("session")
        return session if isinstance(session, dict) else None

    def save_session(self, session_payload: dict[str, Any]) -> bool:
        rsp = self._request_json("POST", "/api/coord/session", {"session": session_payload})
        return bool(rsp.get("ok"))

    def upload_artifact(
        self,
        root_domain: str,
        artifact_type: str,
        content: bytes,
        *,
        source_worker: str,
        content_encoding: str = "identity",
    ) -> bool:
        rsp = self._request_json(
            "POST",
            "/api/coord/artifact",
            {
                "root_domain": root_domain,
                "artifact_type": artifact_type,
                "source_worker": source_worker,
                "content_encoding": content_encoding,
                "content_base64": base64.b64encode(bytes(content)).decode("ascii"),
            },
        )
        return bool(rsp.get("ok"))

    def download_artifact(self, root_domain: str, artifact_type: str) -> dict[str, Any] | None:
        query = urlencode({"root_domain": root_domain, "artifact_type": artifact_type})
        rsp = self._request_json("GET", f"/api/coord/artifact?{query}")
        if not bool(rsp.get("found")):
            return None
        artifact = rsp.get("artifact")
        if not isinstance(artifact, dict):
            return None
        data_b64 = str(artifact.get("content_base64", "") or "")
        try:
            raw = base64.b64decode(data_b64.encode("ascii"), validate=True)
        except Exception:
            return None
        return {
            **artifact,
            "content": raw,
        }

    def get_fleet_settings(self) -> dict[str, Any]:
        return self._request_json("GET", "/api/coord/fleet-settings")


class LeaseHeartbeat(threading.Thread):
    def __init__(self, tick_fn, interval_seconds: float):
        super().__init__(daemon=True)
        self._tick_fn = tick_fn
        self._interval = max(5.0, float(interval_seconds))
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._tick_fn()
            except Exception:
                continue

    def stop(self) -> None:
        self._stop.set()


class SessionUploader(threading.Thread):
    def __init__(
        self,
        client: CoordinatorClient,
        *,
        root_domain: str,
        session_path: Path,
        interval_seconds: float,
        stop_event: threading.Event,
    ):
        super().__init__(daemon=True)
        self.client = client
        self.root_domain = root_domain
        self.session_path = session_path
        self.interval = max(5.0, float(interval_seconds))
        self.stop_event = stop_event
        self.last_mtime_ns = 0

    def run(self) -> None:
        while not self.stop_event.wait(self.interval):
            self.upload_once()

    def upload_once(self) -> None:
        if not self.session_path.is_file():
            return
        try:
            st = self.session_path.stat()
        except OSError:
            return
        if st.st_mtime_ns <= self.last_mtime_ns:
            return
        data = _read_json_dict(self.session_path)
        if not data:
            return
        data["root_domain"] = self.root_domain
        data["saved_at_utc"] = str(data.get("saved_at_utc") or _now_iso())
        if self.client.save_session(data):
            self.last_mtime_ns = st.st_mtime_ns


def _zip_directory_bytes(path: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for file_path in sorted(path.rglob("*")):
            if file_path.is_dir():
                continue
            rel = file_path.relative_to(path).as_posix()
            zf.writestr(rel, file_path.read_bytes())
    return buf.getvalue()


def _unzip_bytes_to_directory(content: bytes, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(content), mode="r") as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            member_name = member.filename.replace("\\", "/").lstrip("/")
            out_path = (target_dir / member_name).resolve()
            try:
                out_path.relative_to(target_dir.resolve())
            except ValueError:
                continue
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(zf.read(member))


def run_subprocess(cmd: list[str], *, cwd: Path, log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_handle:
        log_handle.write(f"\n=== RUN {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n$ {' '.join(cmd)}\n")
        log_handle.flush()
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        return int(proc.wait())


class DistributedCoordinator:
    def __init__(self, cfg: CoordinatorConfig):
        self.cfg = cfg
        self.client = CoordinatorClient(cfg.server_base_url, cfg.api_token)
        host = socket.gethostname().strip() or "worker"
        self.worker_prefix = f"{host}-{uuid.uuid4().hex[:8]}"
        self.stop_event = threading.Event()
        self._job_lock = threading.Lock()
        self._active_jobs = 0
        self._fleet_lock = threading.Lock()

    def _begin_job(self) -> None:
        with self._job_lock:
            self._active_jobs += 1

    def _end_job(self) -> None:
        with self._job_lock:
            self._active_jobs = max(0, self._active_jobs - 1)

    def _read_local_fleet_generation(self) -> int:
        path = self.cfg.output_root / FLEET_GEN_APPLIED_FILENAME
        try:
            text = path.read_text(encoding="utf-8").strip()
            return max(0, int((text.splitlines() or ["0"])[0]))
        except Exception:
            return 0

    def _write_local_fleet_generation(self, generation: int) -> None:
        path = self.cfg.output_root / FLEET_GEN_APPLIED_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{max(0, int(generation))}\n", encoding="utf-8")

    def _maybe_apply_fleet_output_clear(self) -> None:
        """When central bumps output_clear_generation, delete local output/ children (best-effort).

        Intended for drained or idle periods; clearing while jobs run can delete active work.
        """
        with self._fleet_lock:
            with self._job_lock:
                if self._active_jobs > 0:
                    return
            try:
                remote = int(self.client.get_fleet_settings().get("output_clear_generation", 0))
            except Exception:
                return
            local = self._read_local_fleet_generation()
            if remote <= local:
                return
            with self._job_lock:
                if self._active_jobs > 0:
                    return
            print(
                f"[coordinator] fleet local output clear generation {local} -> {remote} under {self.cfg.output_root}",
                flush=True,
            )
            clear_output_root_children(
                self.cfg.output_root,
                preserve_names=frozenset({FLEET_GEN_APPLIED_FILENAME}),
            )
            self._write_local_fleet_generation(remote)

    def _artifact_paths(self, root_domain: str) -> dict[str, Path]:
        domain_dir = _domain_output_dir(root_domain, self.cfg.output_root)
        fozzy_domain_dir = domain_dir / "fozzy-output" / root_domain
        return {
            "nightmare_session_json": domain_dir / f"{root_domain}_crawl_session.json",
            "nightmare_url_inventory_json": domain_dir / f"{root_domain}_url_inventory.json",
            "nightmare_requests_json": domain_dir / "requests.json",
            "nightmare_parameters_json": domain_dir / f"{root_domain}.parameters.json",
            "nightmare_parameters_txt": domain_dir / f"{root_domain}.parameters.txt",
            "nightmare_source_of_truth_json": domain_dir / f"{root_domain}_source_of_truth.json",
            "nightmare_report_html": domain_dir / "report.html",
            "nightmare_log": domain_dir / f"{root_domain}_nightmare.log",
            "nightmare_scrapy_log": domain_dir / f"{root_domain}_scrapy.log",
            "fozzy_summary_json": fozzy_domain_dir / f"{root_domain}.fozzy.summary.json",
            "fozzy_inventory_json": fozzy_domain_dir / f"{root_domain}.fozzy.inventory.json",
            "fozzy_log": fozzy_domain_dir / f"{root_domain}.fozzy.log",
            "fozzy_results_dir": fozzy_domain_dir / "results",
            "extractor_summary_json": fozzy_domain_dir / "extractor" / "summary.json",
            "extractor_matches_dir": fozzy_domain_dir / "extractor" / "matches",
        }

    def _upload_file_artifact(self, root_domain: str, artifact_type: str, path: Path, worker_id: str) -> None:
        if not path.is_file():
            return
        self.client.upload_artifact(root_domain, artifact_type, path.read_bytes(), source_worker=worker_id)

    def _upload_zip_artifact(self, root_domain: str, artifact_type: str, path: Path, worker_id: str) -> None:
        if not path.is_dir():
            return
        payload = _zip_directory_bytes(path)
        self.client.upload_artifact(
            root_domain,
            artifact_type,
            payload,
            source_worker=worker_id,
            content_encoding="zip",
        )

    def _download_file_artifact(self, root_domain: str, artifact_type: str, path: Path) -> bool:
        artifact = self.client.download_artifact(root_domain, artifact_type)
        if artifact is None:
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(bytes(artifact["content"]))
        return True

    def _download_zip_artifact(self, root_domain: str, artifact_type: str, target_dir: Path) -> bool:
        artifact = self.client.download_artifact(root_domain, artifact_type)
        if artifact is None:
            return False
        _unzip_bytes_to_directory(bytes(artifact["content"]), target_dir)
        return True

    def _nightmare_worker_loop(self, idx: int) -> None:
        worker_id = f"{self.worker_prefix}-nightmare-{idx}"
        while not self.stop_event.is_set():
            self._maybe_apply_fleet_output_clear()
            try:
                entry = self.client.claim_target(worker_id, self.cfg.lease_seconds)
            except Exception as exc:
                print(f"[coordinator] nightmare claim failed: {exc}", flush=True)
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            if not entry:
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue

            entry_id = str(entry.get("entry_id", "") or "")
            start_url = str(entry.get("start_url", "") or "")
            root_domain = str(entry.get("root_domain", "") or "").strip().lower()
            if not entry_id or not start_url or not root_domain:
                self.stop_event.wait(1.0)
                continue
            self._begin_job()
            try:
                paths = self._artifact_paths(root_domain)
                domain_dir = _domain_output_dir(root_domain, self.cfg.output_root)
                domain_dir.mkdir(parents=True, exist_ok=True)

                try:
                    remote_session = self.client.load_session(root_domain)
                    if isinstance(remote_session, dict) and remote_session:
                        session_payload = dict(remote_session.get("payload", remote_session))
                        if session_payload:
                            _atomic_write_json(paths["nightmare_session_json"], session_payload)
                except Exception:
                    pass

                heartbeat = LeaseHeartbeat(
                    tick_fn=lambda: self.client.heartbeat_target(entry_id, worker_id, self.cfg.lease_seconds),
                    interval_seconds=self.cfg.heartbeat_interval_seconds,
                )
                heartbeat.start()

                uploader_stop = threading.Event()
                session_uploader = SessionUploader(
                    self.client,
                    root_domain=root_domain,
                    session_path=paths["nightmare_session_json"],
                    interval_seconds=self.cfg.upload_session_every_seconds,
                    stop_event=uploader_stop,
                )
                session_uploader.start()

                cmd = [
                    self.cfg.python_executable,
                    "nightmare.py",
                    start_url,
                    "--config",
                    str(self.cfg.nightmare_config),
                    "--resume",
                ]
                log_path = domain_dir / "coordinator.nightmare.log"
                exit_code = 1
                err_text = ""
                try:
                    exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
                    if exit_code != 0:
                        err_text = f"nightmare exit code {exit_code}"
                except Exception as exc:
                    err_text = str(exc)
                    exit_code = 1
                finally:
                    uploader_stop.set()
                    session_uploader.upload_once()
                    heartbeat.stop()

                try:
                    self._upload_file_artifact(root_domain, "nightmare_session_json", paths["nightmare_session_json"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_url_inventory_json", paths["nightmare_url_inventory_json"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_requests_json", paths["nightmare_requests_json"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_parameters_json", paths["nightmare_parameters_json"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_parameters_txt", paths["nightmare_parameters_txt"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_source_of_truth_json", paths["nightmare_source_of_truth_json"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_report_html", paths["nightmare_report_html"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_log", paths["nightmare_log"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_scrapy_log", paths["nightmare_scrapy_log"], worker_id)
                except Exception as exc:
                    print(f"[coordinator] nightmare artifact upload failed ({root_domain}): {exc}", flush=True)

                if exit_code == 0:
                    try:
                        self.client.enqueue_stage(root_domain, "fozzy")
                    except Exception:
                        pass
                try:
                    self.client.complete_target(entry_id, worker_id, exit_code, err_text)
                except Exception as exc:
                    print(f"[coordinator] nightmare complete failed ({root_domain}): {exc}", flush=True)
            finally:
                self._end_job()

    def _fozzy_worker_loop(self, idx: int) -> None:
        worker_id = f"{self.worker_prefix}-fozzy-{idx}"
        while not self.stop_event.is_set():
            self._maybe_apply_fleet_output_clear()
            try:
                entry = self.client.claim_stage(worker_id, "fozzy", self.cfg.lease_seconds)
            except Exception as exc:
                print(f"[coordinator] fozzy claim failed: {exc}", flush=True)
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            if not entry:
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            root_domain = str(entry.get("root_domain", "") or "").strip().lower()
            if not root_domain:
                self.stop_event.wait(1.0)
                continue
            paths = self._artifact_paths(root_domain)
            params_path = paths["nightmare_parameters_json"]
            if not params_path.is_file():
                self._download_file_artifact(root_domain, "nightmare_parameters_json", params_path)
            if not params_path.is_file():
                self.client.complete_stage(worker_id, root_domain, "fozzy", 1, "missing parameters artifact")
                continue

            self._begin_job()
            try:
                heartbeat = LeaseHeartbeat(
                    tick_fn=lambda: self.client.heartbeat_stage(worker_id, root_domain, "fozzy", self.cfg.lease_seconds),
                    interval_seconds=self.cfg.heartbeat_interval_seconds,
                )
                heartbeat.start()
                cmd = [
                    self.cfg.python_executable,
                    "fozzy.py",
                    str(params_path),
                    "--config",
                    str(self.cfg.fozzy_config),
                    "--max-background-workers",
                    str(max(1, int(self.cfg.fozzy_process_workers))),
                    "--max-workers-per-domain",
                    "1",
                    "--max-workers-per-subdomain",
                    "1",
                ]
                log_path = _domain_output_dir(root_domain, self.cfg.output_root) / "coordinator.fozzy.log"
                exit_code = 1
                err_text = ""
                try:
                    exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
                    if exit_code != 0:
                        err_text = f"fozzy exit code {exit_code}"
                except Exception as exc:
                    err_text = str(exc)
                    exit_code = 1
                finally:
                    heartbeat.stop()

                try:
                    self._upload_file_artifact(root_domain, "fozzy_summary_json", paths["fozzy_summary_json"], worker_id)
                    self._upload_file_artifact(root_domain, "fozzy_inventory_json", paths["fozzy_inventory_json"], worker_id)
                    self._upload_file_artifact(root_domain, "fozzy_log", paths["fozzy_log"], worker_id)
                    self._upload_zip_artifact(root_domain, "fozzy_results_zip", paths["fozzy_results_dir"], worker_id)
                except Exception as exc:
                    print(f"[coordinator] fozzy artifact upload failed ({root_domain}): {exc}", flush=True)

                if exit_code == 0:
                    try:
                        self.client.enqueue_stage(root_domain, "extractor")
                    except Exception:
                        pass
                try:
                    self.client.complete_stage(worker_id, root_domain, "fozzy", exit_code, err_text)
                except Exception as exc:
                    print(f"[coordinator] fozzy complete failed ({root_domain}): {exc}", flush=True)
            finally:
                self._end_job()

    def _extractor_worker_loop(self, idx: int) -> None:
        worker_id = f"{self.worker_prefix}-extractor-{idx}"
        while not self.stop_event.is_set():
            self._maybe_apply_fleet_output_clear()
            try:
                entry = self.client.claim_stage(worker_id, "extractor", self.cfg.lease_seconds)
            except Exception as exc:
                print(f"[coordinator] extractor claim failed: {exc}", flush=True)
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            if not entry:
                self.stop_event.wait(self.cfg.poll_interval_seconds)
                continue
            root_domain = str(entry.get("root_domain", "") or "").strip().lower()
            if not root_domain:
                self.stop_event.wait(1.0)
                continue
            paths = self._artifact_paths(root_domain)
            domain_dir = _domain_output_dir(root_domain, self.cfg.output_root)
            fozzy_results_dir = paths["fozzy_results_dir"]
            if not fozzy_results_dir.is_dir():
                self._download_zip_artifact(root_domain, "fozzy_results_zip", fozzy_results_dir)
            if not fozzy_results_dir.is_dir():
                self.client.complete_stage(worker_id, root_domain, "extractor", 1, "missing fozzy results artifact")
                continue

            self._begin_job()
            try:
                heartbeat = LeaseHeartbeat(
                    tick_fn=lambda: self.client.heartbeat_stage(worker_id, root_domain, "extractor", self.cfg.lease_seconds),
                    interval_seconds=self.cfg.heartbeat_interval_seconds,
                )
                heartbeat.start()
                cmd = [
                    self.cfg.python_executable,
                    "extractor.py",
                    root_domain,
                    "--config",
                    str(self.cfg.extractor_config),
                    "--scan-root",
                    str(self.cfg.output_root),
                    "--workers",
                    str(max(1, int(self.cfg.extractor_process_workers))),
                    "--force",
                ]
                log_path = domain_dir / "coordinator.extractor.log"
                exit_code = 1
                err_text = ""
                try:
                    exit_code = run_subprocess(cmd, cwd=BASE_DIR, log_path=log_path)
                    if exit_code != 0:
                        err_text = f"extractor exit code {exit_code}"
                except Exception as exc:
                    err_text = str(exc)
                    exit_code = 1
                finally:
                    heartbeat.stop()

                try:
                    self._upload_file_artifact(root_domain, "extractor_summary_json", paths["extractor_summary_json"], worker_id)
                    self._upload_zip_artifact(root_domain, "extractor_matches_zip", paths["extractor_matches_dir"], worker_id)
                except Exception as exc:
                    print(f"[coordinator] extractor artifact upload failed ({root_domain}): {exc}", flush=True)

                try:
                    self.client.complete_stage(worker_id, root_domain, "extractor", exit_code, err_text)
                except Exception as exc:
                    print(f"[coordinator] extractor complete failed ({root_domain}): {exc}", flush=True)
            finally:
                self._end_job()

    def run(self) -> int:
        threads: list[threading.Thread] = []
        if self.cfg.enable_nightmare:
            for idx in range(1, max(1, self.cfg.nightmare_workers) + 1):
                t = threading.Thread(target=self._nightmare_worker_loop, args=(idx,), daemon=True)
                threads.append(t)
        if self.cfg.enable_fozzy:
            for idx in range(1, max(1, self.cfg.fozzy_workers) + 1):
                t = threading.Thread(target=self._fozzy_worker_loop, args=(idx,), daemon=True)
                threads.append(t)
        if self.cfg.enable_extractor:
            for idx in range(1, max(1, self.cfg.extractor_workers) + 1):
                t = threading.Thread(target=self._extractor_worker_loop, args=(idx,), daemon=True)
                threads.append(t)

        for t in threads:
            t.start()
        print(
            f"[coordinator] started worker_prefix={self.worker_prefix} "
            f"nightmare={self.cfg.nightmare_workers if self.cfg.enable_nightmare else 0} "
            f"fozzy={self.cfg.fozzy_workers if self.cfg.enable_fozzy else 0} "
            f"extractor={self.cfg.extractor_workers if self.cfg.enable_extractor else 0}",
            flush=True,
        )
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            print("\n[coordinator] interrupt received; shutting down.", flush=True)
            self.stop_event.set()
            return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Distributed coordinator worker for Nightmare/Fozzy/Extractor")
    p.add_argument("--config", default=str(CONFIG_PATH_DEFAULT), help="Path to coordinator config JSON")
    p.add_argument("--server-base-url", default=None, help="Coordinator server base URL (e.g. https://coord.example.com)")
    p.add_argument("--api-token", default=None, help="Coordinator API bearer token")
    p.add_argument("--output-root", default=None, help="Output root (default: ./output)")
    return p.parse_args(argv)


def load_config(args: argparse.Namespace) -> CoordinatorConfig:
    config_path = Path(str(args.config)).expanduser()
    if not config_path.is_absolute():
        config_path = (BASE_DIR / config_path).resolve()
    cfg = _read_json_dict(config_path)

    server_base_url = str(
        _merged_value(args.server_base_url, cfg, "server_base_url", os.getenv("COORDINATOR_BASE_URL", ""))
        or ""
    ).strip().rstrip("/")
    if not server_base_url:
        raise ValueError("server_base_url is required")
    api_token = str(
        _merged_value(args.api_token, cfg, "api_token", os.getenv("COORDINATOR_API_TOKEN", ""))
        or ""
    ).strip()
    output_root_raw = str(_merged_value(args.output_root, cfg, "output_root", str(OUTPUT_ROOT_DEFAULT)) or str(OUTPUT_ROOT_DEFAULT))
    output_root = Path(output_root_raw).expanduser()
    if not output_root.is_absolute():
        output_root = (BASE_DIR / output_root).resolve()

    return CoordinatorConfig(
        server_base_url=server_base_url,
        api_token=api_token,
        output_root=output_root,
        heartbeat_interval_seconds=max(5.0, _safe_float(cfg.get("heartbeat_interval_seconds", 20.0), 20.0)),
        lease_seconds=max(30, _safe_int(cfg.get("lease_seconds", 180), 180)),
        poll_interval_seconds=max(1.0, _safe_float(cfg.get("poll_interval_seconds", 5.0), 5.0)),
        nightmare_workers=max(1, _safe_int(cfg.get("nightmare_workers", 2), 2)),
        fozzy_workers=max(1, _safe_int(cfg.get("fozzy_workers", 2), 2)),
        extractor_workers=max(1, _safe_int(cfg.get("extractor_workers", 2), 2)),
        python_executable=str(_merged_value(None, cfg, "python_executable", sys.executable) or sys.executable),
        nightmare_config=(BASE_DIR / str(cfg.get("nightmare_config", "config/nightmare.json"))).resolve(),
        fozzy_config=(BASE_DIR / str(cfg.get("fozzy_config", "config/fozzy.json"))).resolve(),
        extractor_config=(BASE_DIR / str(cfg.get("extractor_config", "config/extractor.json"))).resolve(),
        upload_session_every_seconds=max(5.0, _safe_float(cfg.get("upload_session_every_seconds", 15.0), 15.0)),
        enable_nightmare=bool(cfg.get("enable_nightmare", True)),
        enable_fozzy=bool(cfg.get("enable_fozzy", True)),
        enable_extractor=bool(cfg.get("enable_extractor", True)),
        fozzy_process_workers=max(1, _safe_int(cfg.get("fozzy_process_workers", 1), 1)),
        extractor_process_workers=max(1, _safe_int(cfg.get("extractor_process_workers", 1), 1)),
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args)
    runner = DistributedCoordinator(cfg)
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
