#!/usr/bin/env python3
import argparse
import json
import os
import signal
import socket
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

INITIAL_ACCOUNTS = {
    "A1001": 1000.0,
    "A1002": 2500.0,
    "A1003": 500.0,
    "A1004": 10000.0,
}


class BankingDatabase:
    """مدیریت وضعیت مشترک و تضمین چهار ویژگی ACID با SQLite + Lock"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.RLock()
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=FULL;")
        return conn

    def _init_db(self):
        with self.lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS accounts (
                        account_id TEXT PRIMARY KEY,
                        balance REAL NOT NULL CHECK(balance >= 0)
                    );
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS processed_tx (
                        tx_id TEXT PRIMARY KEY,
                        response_json TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    """
                )
                # مقداردهی اولیه حساب‌ها در صورت خالی بودن دیتابیس
                cur = conn.execute("SELECT COUNT(*) FROM accounts;")
                if cur.fetchone()[0] == 0:
                    for acc, bal in INITIAL_ACCOUNTS.items():
                        conn.execute(
                            "INSERT INTO accounts (account_id, balance) VALUES (?, ?);",
                            (acc, bal),
                        )
                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def get_all_state(self):
        with self.lock:
            conn = self._get_conn()
            try:
                accs = dict(conn.execute("SELECT account_id, balance FROM accounts;").fetchall())
                txs = dict(conn.execute("SELECT tx_id, response_json FROM processed_tx;").fetchall())
                return {"accounts": accs, "processed_tx": txs}
            finally:
                conn.close()

    def sync_full_state(self, state: dict):
        with self.lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                for acc, bal in state.get("accounts", {}).items():
                    conn.execute(
                        "INSERT OR REPLACE INTO accounts (account_id, balance) VALUES (?, ?);",
                        (acc, float(bal)),
                    )
                for tx_id, resp_json in state.get("processed_tx", {}).items():
                    conn.execute(
                        "INSERT OR REPLACE INTO processed_tx (tx_id, response_json, created_at) VALUES (?, ?, ?);",
                        (tx_id, resp_json, datetime.now(timezone.utc).isoformat()),
                    )
                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def apply_replicated_tx(self, tx_id: str, account: str, new_balance: float, response_json: str):
        with self.lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute(
                    "UPDATE accounts SET balance = ? WHERE account_id = ?;",
                    (new_balance, account),
                )
                if tx_id:
                    conn.execute(
                        "INSERT OR REPLACE INTO processed_tx (tx_id, response_json, created_at) VALUES (?, ?, ?);",
                        (tx_id, response_json, datetime.now(timezone.utc).isoformat()),
                    )
                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def execute_transaction(self, action: str, account: str, amount=None, tx_id: str = None, simulate_fail: bool = False):
        """اجرای اتمیک، سازگار، ایزوله و پایدار تراکنش بانکی"""
        with self.lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE;")

                # بررسی Idempotency (جلوگیری از اعمال دوبار یک تراکنش در زمان Failover)
                if tx_id:
                    cur = conn.execute(
                        "SELECT response_json FROM processed_tx WHERE tx_id = ?;", (tx_id,)
                    )
                    row = cur.fetchone()
                    if row:
                        conn.execute("COMMIT;")
                        cached_resp = json.loads(row[0])
                        cached_resp["idempotent_replay"] = True
                        return cached_resp, True

                # بررسی وجود حساب
                cur = conn.execute(
                    "SELECT balance FROM accounts WHERE account_id = ?;", (account,)
                )
                row = cur.fetchone()
                if row is None:
                    conn.execute("ROLLBACK;")
                    return {
                        "status": "error",
                        "error_code": "UNKNOWN_ACCOUNT",
                        "message": f"Account '{account}' does not exist.",
                    }, False

                current_balance = float(row[0])

                if action == "balance":
                    conn.execute("COMMIT;")
                    return {
                        "status": "success",
                        "account": account,
                        "balance": round(current_balance, 2),
                    }, False

                elif action == "deposit":
                    new_balance = round(current_balance + amount, 2)
                    conn.execute(
                        "UPDATE accounts SET balance = ? WHERE account_id = ?;",
                        (new_balance, account),
                    )
                    if simulate_fail:
                        raise RuntimeError("Simulated halfway crash/exception before commit!")

                elif action == "withdraw":
                    if current_balance < amount:
                        conn.execute("ROLLBACK;")
                        return {
                            "status": "error",
                            "error_code": "INSUFFICIENT_FUNDS",
                            "message": f"Insufficient funds: balance is {current_balance}, requested {amount}.",
                            "account": account,
                            "balance": round(current_balance, 2),
                        }, False
                    new_balance = round(current_balance - amount, 2)
                    conn.execute(
                        "UPDATE accounts SET balance = ? WHERE account_id = ?;",
                        (new_balance, account),
                    )
                    if simulate_fail:
                        raise RuntimeError("Simulated halfway crash/exception before commit!")

                response = {
                    "status": "success",
                    "account": account,
                    "balance": new_balance,
                }
                if tx_id:
                    conn.execute(
                        "INSERT INTO processed_tx (tx_id, response_json, created_at) VALUES (?, ?, ?);",
                        (tx_id, json.dumps(response), datetime.now(timezone.utc).isoformat()),
                    )

                # نوشتن قطعی روی دیسک قبل از ارسال پاسخ به کلاینت (Durability)
                conn.execute("COMMIT;")
                return response, False

            except Exception as e:
                conn.execute("ROLLBACK;")
                return {
                    "status": "error",
                    "error_code": "TRANSACTION_ABORTED",
                    "message": f"Transaction aborted and rolled back safely: {str(e)}",
                }, False
            finally:
                conn.close()


class BankingServer:
    def __init__(self, host, port, max_clients, db_path, log_file, role, peer_addr, witness_addr):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.client_semaphore = threading.BoundedSemaphore(max_clients)
        self.db = BankingDatabase(db_path)
        self.log_file = log_file
        self.log_lock = threading.Lock()

        # تنظیمات بخش امتیازی (Cluster & Split-Brain Protection)
        self.configured_role = role  # 'standalone', 'primary', 'backup'
        self.current_role = role
        self.peer_addr = self._parse_addr(peer_addr) if peer_addr else None
        self.witness_addr = self._parse_addr(witness_addr) if witness_addr else None
        self.peer_alive = False
        self.running = True

    @staticmethod
    def _parse_addr(addr_str):
        h, p = addr_str.split(":")
        return h.strip(), int(p.strip())

    def write_log(self, req_id, client_ip, action, account, result, balance, latency_ms, extra=""):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        bal_str = f" balance={balance}" if balance is not None else ""
        extra_str = f" info={extra}" if extra else ""
        log_line = (
            f"{ts} req={req_id} ip={client_ip} action={action} "
            f"account={account} result={result}{bal_str} latency={latency_ms:.2f}ms{extra_str}"
        )
        with self.log_lock:
            print(log_line, flush=True)
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(log_line + "\n")

    def _send_rpc(self, addr, payload, timeout=1.5):
        if not addr:
            return None
        try:
            with socket.create_connection(addr, timeout=timeout) as s:
                s.sendall((json.dumps(payload) + "\n").encode("utf-8"))
                data = s.recv(65536).decode("utf-8").strip()
                return json.loads(data) if data else None
        except Exception:
            return None

    def _has_quorum(self):
        """جلوگیری از Split-Brain در بخش امتیازی دوم (+15%):
        یک سرور تنها زمانی مجاز به نوشتن به عنوان Primary است که یا ارتباطش با سرور همتا برقرار باشد
        یا بتواند گره شاهد (Witness) را ببیند و قفل رهبری را تایید کند."""
        if self.configured_role == "standalone":
            return True
        # بررسی ارتباط با سرور همتا
        peer_pong = self._send_rpc(self.peer_addr, {"internal_cmd": "ping"}, timeout=0.8)
        if peer_pong and peer_pong.get("status") == "ok":
            self.peer_alive = True
            return True
        self.peer_alive = False
        # اگر همتا در دسترس نیست (کرش کرده یا شبکه قطع شده)، بررسی گره Witness برای تشخیص Split-Brain
        if self.witness_addr:
            w_resp = self._send_rpc(
                self.witness_addr,
                {"internal_cmd": "claim_leader", "node": f"{self.host}:{self.port}"},
                timeout=0.8,
            )
            if w_resp and w_resp.get("leader") == f"{self.host}:{self.port}":
                return True
            return False
        # اگر Witness تنظیم نشده باشد (صرفاً بخش امتیازی اول +10%)، Failover مجاز است
        return True

    def _cluster_monitor_loop(self):
        """پایش وضعیت کلاستر، Failover خودکار و بازیابی پس از وصل مجدد شبکه (Recovery)"""
        while self.running:
            if self.configured_role in ("primary", "backup") and self.peer_addr:
                resp = self._send_rpc(self.peer_addr, {"internal_cmd": "ping"}, timeout=1.0)
                if resp and resp.get("status") == "ok":
                    # وقتی ارتباط شبکه برقرار است یا دوباره وصل شده است:
                    if not self.peer_alive:
                        # همگام‌سازی مجدد (Recovery پس از رفع افراز شبکه)
                        if self.current_role == "primary" and resp.get("role") == "primary":
                            # حل تعارض و بازگشت Backup به نقش اصلی خود
                            if self.configured_role == "backup":
                                self.current_role = "backup"
                        if self.current_role == "primary":
                            state = self.db.get_all_state()
                            self._send_rpc(
                                self.peer_addr,
                                {"internal_cmd": "sync_state", "state": state},
                                timeout=2.5,
                            )
                    self.peer_alive = True
                else:
                    self.peer_alive = False
                    if self.current_role == "backup":
                        # اگر Primary از دسترس خارج شد، در صورت داشتن Quorum به Primary ارتقا می‌یابد
                        if self._has_quorum():
                            self.current_role = "primary"
                            print("[CLUSTER] Primary unreachable! Promoted BACKUP -> PRIMARY.", flush=True)
            time.sleep(1.0)

    def validate_and_process(self, raw_bytes: bytes, client_ip: str):
        start_time = time.perf_counter()
        req_id = uuid.uuid4().hex[:6]

        # ۱. بررسی درخواست بدفرم (Malformed request / broken JSON / random bytes)
        try:
            text = raw_bytes.decode("utf-8").strip()
            if not text:
                raise ValueError("Empty payload")
            req = json.loads(text)
            if not isinstance(req, dict):
                raise ValueError("JSON root must be an object")
        except Exception:
            latency = (time.perf_counter() - start_time) * 1000
            self.write_log(req_id, client_ip, "malformed", "N/A", "error:MALFORMED_REQUEST", None, latency)
            return {
                "status": "error",
                "error_code": "MALFORMED_REQUEST",
                "message": "Invalid or malformed JSON request.",
            }

        # بررسی دستورات داخلی کلاستر (Replication / Heartbeat)
        if "internal_cmd" in req:
            cmd = req["internal_cmd"]
            if cmd == "ping":
                return {"status": "ok", "role": self.current_role}
            elif cmd == "replicate_tx":
                self.db.apply_replicated_tx(
                    req.get("tx_id"), req["account"], req["balance"], req["response_json"]
                )
                return {"status": "ok"}
            elif cmd == "sync_state":
                self.db.sync_full_state(req.get("state", {}))
                if self.configured_role == "backup":
                    self.current_role = "backup"
                return {"status": "ok"}

        req_id = str(req.get("request_id") or req.get("tx_id") or req_id)[:12]
        action = req.get("action")
        account = req.get("account")

        # ۲. بررسی فیلدهای ضروری (Missing fields)
        if not action or not account:
            latency = (time.perf_counter() - start_time) * 1000
            self.write_log(
                req_id, client_ip, str(action or "missing"), str(account or "missing"),
                "error:MISSING_FIELDS", None, latency
            )
            return {
                "status": "error",
                "error_code": "MISSING_FIELDS",
                "message": "Both 'action' and 'account' fields are required.",
            }

        if not isinstance(action, str) or not isinstance(account, str):
            latency = (time.perf_counter() - start_time) * 1000
            self.write_log(req_id, client_ip, str(action), str(account), "error:INVALID_FIELD_TYPE", None, latency)
            return {
                "status": "error",
                "error_code": "INVALID_FIELD_TYPE",
                "message": "'action' and 'account' must be strings.",
            }

        # ۳. بررسی عملیات پشتیبانی‌نشده (Unsupported action)
        supported_actions = {"balance", "deposit", "withdraw"}
        if action not in supported_actions:
            latency = (time.perf_counter() - start_time) * 1000
            self.write_log(req_id, client_ip, action, account, "error:UNSUPPORTED_ACTION", None, latency)
            return {
                "status": "error",
                "error_code": "UNSUPPORTED_ACTION",
                "message": f"Action '{action}' is not supported. Supported actions: balance, deposit, withdraw.",
            }

        # ۴. بررسی فیلد amount برای deposit و withdraw
        amount = None
        if action in ("deposit", "withdraw"):
            if "amount" not in req or req["amount"] is None:
                latency = (time.perf_counter() - start_time) * 1000
                self.write_log(req_id, client_ip, action, account, "error:MISSING_FIELDS", None, latency)
                return {
                    "status": "error",
                    "error_code": "MISSING_FIELDS",
                    "message": f"Field 'amount' is required for '{action}'.",
                }
            raw_amount = req["amount"]
            if isinstance(raw_amount, bool) or not isinstance(raw_amount, (int, float)):
                latency = (time.perf_counter() - start_time) * 1000
                self.write_log(req_id, client_ip, action, account, "error:INVALID_AMOUNT", None, latency)
                return {
                    "status": "error",
                    "error_code": "INVALID_AMOUNT",
                    "message": "Amount must be a positive number.",
                }
            amount = float(raw_amount)
            if amount <= 0:
                latency = (time.perf_counter() - start_time) * 1000
                self.write_log(req_id, client_ip, action, account, "error:INVALID_AMOUNT", None, latency)
                return {
                    "status": "error",
                    "error_code": "INVALID_AMOUNT",
                    "message": "Amount must be strictly greater than zero.",
                }

        # ۵. بررسی نقش سرور و جلوگیری از Split-Brain در حالت کلاستر
        if self.configured_role != "standalone":
            if self.current_role != "primary":
                latency = (time.perf_counter() - start_time) * 1000
                self.write_log(req_id, client_ip, action, account, "error:NOT_PRIMARY", None, latency)
                return {
                    "status": "error",
                    "error_code": "NOT_PRIMARY",
                    "message": "This node is currently BACKUP. Please contact the PRIMARY server.",
                }
            if action in ("deposit", "withdraw") and not self._has_quorum():
                latency = (time.perf_counter() - start_time) * 1000
                self.write_log(req_id, client_ip, action, account, "error:SPLIT_BRAIN_PREVENTED", None, latency)
                return {
                    "status": "error",
                    "error_code": "QUORUM_LOST",
                    "message": "Network partition detected (No Quorum). Write rejected to prevent Split-Brain.",
                }

        # ۶. اجرای تراکنش روی دیتابیس
        tx_id = req.get("tx_id")
        simulate_fail = bool(req.get("simulate_fail", False))
        resp, was_replayed = self.db.execute_transaction(
            action=action,
            account=account,
            amount=amount,
            tx_id=tx_id,
            simulate_fail=simulate_fail,
        )

        # ۷. تکثیر وضعیت (State Replication) به سرور Backup قبل از بازگشت پاسخ به کلاینت
        if (
            resp.get("status") == "success"
            and action in ("deposit", "withdraw")
            and not was_replayed
            and self.peer_addr
        ):
            self._send_rpc(
                self.peer_addr,
                {
                    "internal_cmd": "replicate_tx",
                    "tx_id": tx_id,
                    "account": account,
                    "balance": resp["balance"],
                    "response_json": json.dumps(resp),
                },
                timeout=1.2,
            )

        latency = (time.perf_counter() - start_time) * 1000
        result_tag = "success" if resp.get("status") == "success" else f"error:{resp.get('error_code')}"
        self.write_log(
            req_id=req_id,
            client_ip=client_ip,
            action=action,
            account=account,
            result=result_tag,
            balance=resp.get("balance"),
            latency_ms=latency,
        )
        return resp

    def handle_client(self, conn: socket.socket, addr):
        client_ip = addr[0]
        try:
            conn.settimeout(30.0)
            buffer = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    if buffer:
                        resp = self.validate_and_process(buffer, client_ip)
                        conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if not line.strip():
                        continue
                    resp = self.validate_and_process(line, client_ip)
                    conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
        except socket.timeout:
            pass
        except Exception as e:
            print(f"[WARN] Connection error with {client_ip}: {e}", flush=True)
        finally:
            try:
                conn.close()
            except Exception:
                pass
            self.client_semaphore.release()

    def serve_forever(self):
        if self.configured_role in ("primary", "backup"):
            t = threading.Thread(target=self._cluster_monitor_loop, daemon=True)
            t.start()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv_sock:
            srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv_sock.bind((self.host, self.port))
            srv_sock.listen(128)
            print(
                f"[*] Banking Server listening on {self.host}:{self.port} | "
                f"role={self.current_role} | max_clients={self.max_clients} | db={self.db.db_path}",
                flush=True,
            )

            while self.running:
                try:
                    conn, addr = srv_sock.accept()
                except OSError:
                    break

                # اعمال محدودیت تعداد کلاینت‌های هم‌زمان (Configurable Max Clients)
                if not self.client_semaphore.acquire(blocking=False):
                    busy_resp = {
                        "status": "error",
                        "error_code": "SERVER_BUSY",
                        "message": f"Maximum simultaneous clients ({self.max_clients}) reached. Try again later.",
                    }
                    try:
                        conn.sendall((json.dumps(busy_resp) + "\n").encode("utf-8"))
                        conn.close()
                    except Exception:
                        pass
                    continue

                t = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
                t.start()


def main():
    parser = argparse.ArgumentParser(description="Mini Banking Transaction Server")
    parser.add_argument("--host", default=os.getenv("BANK_HOST", "0.0.0.0"), help="Bind IP address")
    parser.add_argument("--port", type=int, default=int(os.getenv("BANK_PORT", "9000")), help="Bind TCP port")
    parser.add_argument(
        "--max-clients",
        type=int,
        default=int(os.getenv("MAX_CLIENTS", "50")),
        help="Maximum number of simultaneous clients",
    )
    parser.add_argument("--db", default=os.getenv("BANK_DB", "bank_state.db"), help="SQLite database file path")
    parser.add_argument("--log", default=os.getenv("BANK_LOG", "server.log"), help="Log file path")
    parser.add_argument(
        "--role",
        choices=["standalone", "primary", "backup"],
        default=os.getenv("BANK_ROLE", "standalone"),
        help="Server role (standalone for main project, primary/backup for bonus)",
    )
    parser.add_argument("--peer", default=os.getenv("BANK_PEER", ""), help="Peer server IP:PORT for replication")
    parser.add_argument(
        "--witness", default=os.getenv("BANK_WITNESS", ""), help="Witness node IP:PORT for Split-Brain protection"
    )
    args = parser.parse_args()

    server = BankingServer(
        host=args.host,
        port=args.port,
        max_clients=args.max_clients,
        db_path=args.db,
        log_file=args.log,
        role=args.role,
        peer_addr=args.peer,
        witness_addr=args.witness,
    )

    def sig_handler(sig, frame):
        print("\n[!] Shutting down server gracefully...", flush=True)
        server.running = False
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
