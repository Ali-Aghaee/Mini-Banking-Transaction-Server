#!/usr/bin/env python3
import socket
import sqlite3
import threading
import json
import time
import argparse

class ReplicatedBankDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.RLock()
        self._init_db()

    def _conn(self):
        c = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=FULL;")
        return c

    def _init_db(self):
        with self.lock:
            c = self._conn()
            c.execute("BEGIN IMMEDIATE;")
            c.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id TEXT PRIMARY KEY,
                    balance INTEGER NOT NULL CHECK (balance >= 0)
                );
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS tx_log (
                    seq_no INTEGER PRIMARY KEY AUTOINCREMENT,
                    tx_id TEXT UNIQUE NOT NULL,
                    action TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    resulting_balance INTEGER NOT NULL,
                    response_json TEXT NOT NULL
                );
            """)
            if c.execute("SELECT COUNT(*) FROM accounts;").fetchone()[0] == 0:
                c.executemany("INSERT INTO accounts VALUES (?, ?);", [
                    ("A1001", 1000), ("A1002", 2000), ("A1003", 5000)
                ])
            c.execute("COMMIT;")
            c.close()

    def get_last_seq_no(self) -> int:
        with self.lock:
            c = self._conn()
            row = c.execute("SELECT MAX(seq_no) FROM tx_log;").fetchone()
            c.close()
            return row[0] if row and row[0] is not None else 0

    def get_logs_since(self, last_seq: int):
        with self.lock:
            c = self._conn()
            rows = c.execute(
                "SELECT seq_no, tx_id, action, account_id, amount, resulting_balance, response_json FROM tx_log WHERE seq_no > ? ORDER BY seq_no ASC;",
                (last_seq,)
            ).fetchall()
            c.close()
            return [
                {
                    "seq_no": r[0], "tx_id": r[1], "action": r[2],
                    "account": r[3], "amount": r[4], "balance": r[5],
                    "response": json.loads(r[6])
                } for r in rows
            ]

    def apply_replicated_entry(self, entry: dict):
        with self.lock:
            c = self._conn()
            try:
                c.execute("BEGIN IMMEDIATE;")
                exists = c.execute("SELECT 1 FROM tx_log WHERE tx_id = ?;", (entry["tx_id"],)).fetchone()
                if not exists:
                    c.execute("UPDATE accounts SET balance = ? WHERE account_id = ?;", (entry["balance"], entry["account"]))
                    c.execute(
                        "INSERT INTO tx_log (seq_no, tx_id, action, account_id, amount, resulting_balance, response_json) VALUES (?, ?, ?, ?, ?, ?, ?);",
                        (entry["seq_no"], entry["tx_id"], entry["action"], entry["account"], entry["amount"], entry["balance"], json.dumps(entry["response"]))
                    )
                c.execute("COMMIT;")
                return True
            except Exception as e:
                c.execute("ROLLBACK;")
                print(f"[REPLICATION ERROR] {e}")
                return False
            finally:
                c.close()

    def process_client_tx(self, tx_id: str, action: str, account: str, amount: int = 0):
        with self.lock:
            c = self._conn()
            try:
                c.execute("BEGIN IMMEDIATE;")
                existing = c.execute("SELECT response_json FROM tx_log WHERE tx_id = ?;", (tx_id,)).fetchone()
                if existing:
                    c.execute("COMMIT;")
                    resp = json.loads(existing[0])
                    resp["idempotent_replay"] = True
                    return resp, None

                row = c.execute("SELECT balance FROM accounts WHERE account_id = ?;", (account,)).fetchone()
                if not row:
                    c.execute("ROLLBACK;")
                    return {"status": "error", "error_code": "UNKNOWN_ACCOUNT", "message": "Account not found"}, None

                bal = row[0]
                if action == "balance":
                    c.execute("COMMIT;")
                    return {"status": "success", "account": account, "balance": bal}, None

                if action == "withdraw":
                    if bal < amount:
                        c.execute("ROLLBACK;")
                        return {"status": "error", "error_code": "INSUFFICIENT_FUNDS", "balance": bal}, None
                    new_bal = bal - amount
                elif action == "deposit":
                    new_bal = bal + amount
                else:
                    c.execute("ROLLBACK;")
                    return {"status": "error", "error_code": "UNSUPPORTED_ACTION"}, None

                c.execute("UPDATE accounts SET balance = ? WHERE account_id = ?;", (new_bal, account))
                resp = {"status": "success", "account": account, "balance": new_bal, "tx_id": tx_id}
                cur = c.execute(
                    "INSERT INTO tx_log (tx_id, action, account_id, amount, resulting_balance, response_json) VALUES (?, ?, ?, ?, ?, ?);",
                    (tx_id, action, account, amount, new_bal, json.dumps(resp))
                )
                seq_no = cur.lastrowid
                c.execute("COMMIT;")

                repl_entry = {
                    "seq_no": seq_no, "tx_id": tx_id, "action": action,
                    "account": account, "amount": amount, "balance": new_bal, "response": resp
                }
                return resp, repl_entry
            except Exception as e:
                c.execute("ROLLBACK;")
                return {"status": "error", "message": str(e)}, None
            finally:
                c.close()


def rpc_call(host: str, port: int, payload: dict, timeout=1.5):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            s.sendall((json.dumps(payload) + "\n").encode("utf-8"))
            data = s.recv(65536).decode("utf-8").strip()
            return json.loads(data)
    except Exception:
        return None


class ClusterBankingNode:
    def __init__(self, node_id: str, initial_role: str, host: str, port: int, peer_host: str, peer_port: int, witness_host: str, witness_port: int):
        self.node_id = node_id
        self.role = initial_role
        self.host = host
        self.port = port
        self.peer_host = peer_host
        self.peer_port = peer_port
        self.witness_host = witness_host
        self.witness_port = witness_port
        self.db = ReplicatedBankDB(f"cluster_{node_id}.db")
        self.peer_alive = True

    def verify_quorum_lease(self) -> bool:
        res = rpc_call(self.witness_host, self.witness_port, {
            "internal_cmd": "ACQUIRE_LEASE",
            "node_id": self.node_id
        }, timeout=1.0)
        if res and res.get("lease_granted") is True:
            return True
        return False

    def monitor_cluster_loop(self):
        while True:
            time.sleep(1.5)
            peer_status = rpc_call(self.peer_host, self.peer_port, {
                "internal_cmd": "HEARTBEAT",
                "sender_id": self.node_id,
                "sender_role": self.role,
                "last_seq": self.db.get_last_seq_no()
            }, timeout=1.2)

            if peer_status and peer_status.get("status") == "ok":
                self.peer_alive = True
                peer_seq = peer_status.get("last_seq", 0)
                my_seq = self.db.get_last_seq_no()

                if peer_seq > my_seq:
                    print(f"[{self.node_id}] Detected higher sequence on peer ({peer_seq} > {my_seq}). Syncing delta logs...")
                    sync_res = rpc_call(self.peer_host, self.peer_port, {
                        "internal_cmd": "FETCH_LOGS",
                        "since_seq": my_seq
                    }, timeout=3.0)
                    if sync_res and "logs" in sync_res:
                        for entry in sync_res["logs"]:
                            self.db.apply_replicated_entry(entry)
                        print(f"[{self.node_id}] State Catch-Up Completed! Current Seq: {self.db.get_last_seq_no()}")

                if self.role == "PRIMARY" and peer_status.get("role") == "PRIMARY":
                    if not self.verify_quorum_lease():
                        print(f"[{self.node_id}] Stepping down to BACKUP after partition heal.")
                        self.role = "BACKUP"
                elif self.role == "FENCED":
                    self.role = "BACKUP"
                    print(f"[{self.node_id}] Network partition healed. Transitioned from FENCED -> BACKUP.")

            else:
                self.peer_alive = False
                if self.role == "PRIMARY":
                    if self.verify_quorum_lease():
                        pass
                    else:
                        print(f"[{self.node_id}] Lost Quorum during partition! Stepping down to FENCED to prevent Split-Brain.")
                        self.role = "FENCED"

                elif self.role in {"BACKUP", "FENCED"}:
                    if self.verify_quorum_lease():
                        print(f"[{self.node_id}] Primary unreachable & Witness Lease Acquired! Promoting to PRIMARY!")
                        self.role = "PRIMARY"
                    else:
                        if self.role != "FENCED":
                            print(f"[{self.node_id}] Cannot reach Primary & Lease denied by Witness (Network Partition detected). Entering FENCED mode (Split-Brain prevented)!")
                            self.role = "FENCED"

    def handle_connection(self, conn: socket.socket, addr):
        try:
            raw = conn.recv(65536).decode("utf-8").strip()
            if not raw:
                return
            msg = json.loads(raw)

            if "internal_cmd" in msg:
                cmd = msg["internal_cmd"]
                if cmd == "HEARTBEAT":
                    resp = {"status": "ok", "node_id": self.node_id, "role": self.role, "last_seq": self.db.get_last_seq_no()}
                elif cmd == "REPLICATE":
                    ok = self.db.apply_replicated_entry(msg["entry"])
                    resp = {"status": "ok" if ok else "error"}
                elif cmd == "FETCH_LOGS":
                    logs = self.db.get_logs_since(msg["since_seq"])
                    resp = {"status": "ok", "logs": logs}
                else:
                    resp = {"status": "error", "message": "Unknown internal command"}
                conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                return

            action = msg.get("action")
            account = msg.get("account")
            amount = msg.get("amount", 0)
            tx_id = msg.get("tx_id", f"auto-{time.time_ns()}")

            if self.role != "PRIMARY":
                resp = {
                    "status": "error",
                    "error_code": "NOT_PRIMARY_NODE",
                    "node_id": self.node_id,
                    "current_role": self.role,
                    "message": f"Node {self.node_id} is currently {self.role} (Split-Brain Protection Active). Redirect to Primary."
                }
                conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                return

            if not self.peer_alive and not self.verify_quorum_lease():
                self.role = "FENCED"
                resp = {
                    "status": "error",
                    "error_code": "QUORUM_LOST",
                    "message": "Server lost quorum and refused transaction to prevent Split-Brain"
                }
                conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                return

            resp, repl_entry = self.db.process_client_tx(tx_id, action, account, amount)
            resp["served_by"] = self.node_id

            if repl_entry is not None and self.peer_alive:
                rpc_call(self.peer_host, self.peer_port, {
                    "internal_cmd": "REPLICATE",
                    "entry": repl_entry
                }, timeout=1.5)

            print(f"[{self.node_id} | {self.role}] tx={tx_id} action={action} acc={account} -> {resp.get('status')} (bal={resp.get('balance')})")
            conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))

        except Exception as e:
            try:
                conn.sendall((json.dumps({"status": "error", "message": str(e)}) + "\n").encode("utf-8"))
            except Exception:
                pass
        finally:
            conn.close()

    def start(self):
        threading.Thread(target=self.monitor_cluster_loop, daemon=True).start()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(128)
        print("============================================================")
        print(f" Cluster Node [{self.node_id}] started on {self.host}:{self.port} | Initial Role: {self.role}")
        print(f" Peer: {self.peer_host}:{self.peer_port} | Witness: {self.witness_host}:{self.witness_port}")
        print("============================================================")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=self.handle_connection, args=(conn, addr), daemon=True).start()


def run_witness_arbitrator(host: str, port: int):
    state = {"holder": "ServerA", "expires_at": 0.0}
    lock = threading.Lock()

    def handle(conn):
        try:
            raw = conn.recv(4096).decode("utf-8").strip()
            msg = json.loads(raw)
            if msg.get("internal_cmd") == "ACQUIRE_LEASE":
                node_id = msg["node_id"]
                now = time.time()
                with lock:
                    if state["holder"] == node_id or now > state["expires_at"]:
                        if state["holder"] != node_id:
                            print(f"[WITNESS] Lease transferred from {state['holder']} -> {node_id}")
                        state["holder"] = node_id
                        state["expires_at"] = now + 4.0
                        granted = True
                    else:
                        granted = False
                conn.sendall((json.dumps({"lease_granted": granted, "current_holder": state["holder"]}) + "\n").encode("utf-8"))
        except Exception:
            pass
        finally:
            conn.close()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(64)
    print(f"[WITNESS ARBITRATOR] Running on {host}:{port} (Split-Brain Prevention Lease Service)")
    while True:
        c, _ = s.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fault-Tolerant & Split-Brain Resistant Banking Cluster")
    parser.add_argument("--mode", choices=["node", "witness"], required=True)
    parser.add_argument("--node-id", type=str, default="ServerA")
    parser.add_argument("--role", choices=["PRIMARY", "BACKUP"], default="PRIMARY")
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6000)
    parser.add_argument("--peer-host", type=str, default="127.0.0.1")
    parser.add_argument("--peer-port", type=int, default=6001)
    parser.add_argument("--witness-host", type=str, default="127.0.0.1")
    parser.add_argument("--witness-port", type=int, default=7000)
    args = parser.parse_args()

    if args.mode == "witness":
        run_witness_arbitrator(args.host, args.port)
    else:
        node = ClusterBankingNode(
            args.node_id, args.role, args.host, args.port,
            args.peer_host, args.peer_port, args.witness_host, args.witness_port
        )
        node.start()
