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
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

from http_client import request_json

from output_cleanup import FLEET_GEN_APPLIED_FILENAME, clear_output_root_children
from nightmare_shared.config import CoordinatorSettings, atomic_write_json, load_env_file_into_os, merged_value, read_json_dict, safe_float, safe_int
from nightmare_shared.logging_utils import configure_logging, get_logger
from coordinator_app.runtime import CoordinatorClient, CoordinatorConfig, SessionUploader, LeaseHeartbeat, _zip_directory_bytes, _unzip_bytes_to_directory, run_subprocess, load_config

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
        value = cfg[key]
        if value is not None and value != "":
            return value
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


class DistributedCoordinator:
    def __init__(self, cfg: CoordinatorConfig):
        self.cfg = cfg
        # Disable SSL verification for localhost (self-signed cert in development)
        verify_ssl = not cfg.server_base_url.startswith("https://localhost")
        self.client = CoordinatorClient(cfg.server_base_url, cfg.api_token, verify_ssl=verify_ssl)
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
        high_value_dir = self.cfg.output_root / "high_value" / root_domain
        return {
            "nightmare_session_json": domain_dir / f"{root_domain}_crawl_session.json",
            "nightmare_url_inventory_json": domain_dir / f"{root_domain}_url_inventory.json",
            "nightmare_requests_json": domain_dir / "requests.json",
            "nightmare_parameters_json": domain_dir / f"{root_domain}.parameters.json",
            "nightmare_post_requests_json": domain_dir / f"{root_domain}.post.requests.json",
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
            "nightmare_high_value_dir": high_value_dir,
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
                    self._upload_file_artifact(
                        root_domain, "nightmare_post_requests_json", paths["nightmare_post_requests_json"], worker_id
                    )
                    self._upload_file_artifact(root_domain, "nightmare_parameters_txt", paths["nightmare_parameters_txt"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_source_of_truth_json", paths["nightmare_source_of_truth_json"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_report_html", paths["nightmare_report_html"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_log", paths["nightmare_log"], worker_id)
                    self._upload_file_artifact(root_domain, "nightmare_scrapy_log", paths["nightmare_scrapy_log"], worker_id)
                    hv_dir = paths.get("nightmare_high_value_dir")
                    if hv_dir is not None and hv_dir.is_dir():
                        self._upload_zip_artifact(
                            root_domain, "nightmare_high_value_zip", hv_dir, worker_id
                        )
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

            hv_dir = paths.get("nightmare_high_value_dir")
            if hv_dir is not None:
                hv_dir.mkdir(parents=True, exist_ok=True)
                self._download_zip_artifact(root_domain, "nightmare_high_value_zip", hv_dir)
            self._download_file_artifact(root_domain, "nightmare_post_requests_json", paths["nightmare_post_requests_json"])

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


def parse_args(argv: Optional[list[str] ] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Distributed coordinator worker for Nightmare/Fozzy/Extractor")
    p.add_argument("--config", default=str(CONFIG_PATH_DEFAULT), help="Path to coordinator config JSON")
    p.add_argument("--server-base-url", default=None, help="Coordinator server base URL (e.g. https://coord.example.com)")
    p.add_argument("--api-token", default=None, help="Coordinator API bearer token")
    p.add_argument("--output-root", default=None, help="Output root (default: ./output)")
    return p.parse_args(argv)



def main(argv: Optional[list[str] ] = None) -> int:
    configure_logging()
    logger = get_logger("coordinator")
    args = parse_args(argv)
    cfg = load_config(args)
    logger.info(
        "coordinator_starting",
        server_base_url=cfg.server_base_url,
        output_root=str(cfg.output_root),
        nightmare_workers=cfg.nightmare_workers,
        fozzy_workers=cfg.fozzy_workers,
        extractor_workers=cfg.extractor_workers,
    )
    runner = DistributedCoordinator(cfg)
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
