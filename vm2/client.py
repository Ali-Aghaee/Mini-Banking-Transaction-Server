#!/usr/bin/env python3
import socket
import json
import threading
import argparse
import time
import uuid

def send_request(host: str, port: int, payload, raw_bytes: bytes = None):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10.0)
        s.connect((host, port))
        if raw_bytes is not None:
            s.sendall(raw_bytes)
        else:
            msg = json.dumps(payload) + "\n"
            s.sendall(msg.encode("utf-8"))
        
        resp = s.recv(4096).decode("utf-8").strip()
        try:
            return json.loads(resp)
        except Exception:
            return {"raw_response": resp}

def run_full_demo_suite(host: str, port: int):
    print("==========================================================")
    print(f" Starting Automated Verification Suite against {host}:{port}")
    print("==========================================================\n")

    print("--- [1] Testing Normal Operations (Inquiry, Deposit, Withdraw) ---")
    r1 = send_request(host, port, {"action": "balance", "account": "A1001"})
    print(f"Initial Balance A1001: {r1}")
    
    r2 = send_request(host, port, {"action": "deposit", "account": "A1001", "amount": 500})
    print(f"After Deposit +500:    {r2}")

    r3 = send_request(host, port, {"action": "withdraw", "account": "A1001", "amount": 200})
    print(f"After Withdraw -200:   {r3}\n")

    print("--- [2] Testing High Concurrency (20 Simultaneous Withdrawals on A1003) ---")
    init_a1003 = send_request(host, port, {"action": "balance", "account": "A1003"})["balance"]
    print(f"Initial Balance of A1003: {init_a1003}")

    num_threads = 20
    withdraw_amount = 100
    results = []
    lock = threading.Lock()

    def worker():
        res = send_request(host, port, {"action": "withdraw", "account": "A1003", "amount": withdraw_amount})
        with lock:
            results.append(res)

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    success_count = sum(1 for r in results if r.get("status") == "success")
    final_a1003 = send_request(host, port, {"action": "balance", "account": "A1003"})["balance"]
    expected_balance = init_a1003 - (success_count * withdraw_amount)
    print(f"Concurrent Requests: {num_threads} | Successful: {success_count}")
    print(f"Expected Final Balance: {expected_balance} | Actual Final Balance: {final_a1003}")
    assert final_a1003 == expected_balance, "Concurrency mismatch!"
    print(">> Concurrency & Isolation Verified Successfully (Zero Lost Updates)!\n")

    print("--- [3] Testing Atomicity (Mid-Transaction Crash / Rollback) ---")
    bal_before_crash = send_request(host, port, {"action": "balance", "account": "A1002"})["balance"]
    print(f"Balance of A1002 before failed transfer: {bal_before_crash}")
    crash_res = send_request(host, port, {
        "action": "transfer",
        "account": "A1002",
        "target_account": "A1001",
        "amount": 300,
        "simulate_crash": True
    })
    print(f"Failed Mid-Transaction Response: {crash_res}")
    bal_after_crash = send_request(host, port, {"action": "balance", "account": "A1002"})["balance"]
    print(f"Balance of A1002 after rollback:         {bal_after_crash}")
    assert bal_before_crash == bal_after_crash, "Atomicity violated!"
    print(">> Atomicity Verified Successfully!\n")

    print("--- [4] Testing All Required Error Handling Cases ---")
    error_cases = [
        ("Unknown Account", {"action": "balance", "account": "Z9999"}, None),
        ("Insufficient Funds", {"action": "withdraw", "account": "A1001", "amount": 9999999}, None),
        ("Negative Amount", {"action": "deposit", "account": "A1001", "amount": -50}, None),
        ("Zero Amount", {"action": "deposit", "account": "A1001", "amount": 0}, None),
        ("String Amount", {"action": "withdraw", "account": "A1001", "amount": "abc"}, None),
        ("Unsupported Action", {"action": "loan_request", "account": "A1001", "amount": 100}, None),
        ("Missing Fields", {"action": "withdraw", "account": "A1001"}, None),
        ("Malformed JSON", None, b"{broken_json: 123\n"),
        ("Random Socket Bytes", None, b"\xff\xfe\xfd\x00\x89random_garbage_bytes\n")
    ]

    for title, payload, raw in error_cases:
        res = send_request(host, port, payload, raw_bytes=raw)
        print(f"Case [{title:19s}] -> {res}")

    print("\n>> Verify server is still alive after all errors:")
    alive_check = send_request(host, port, {"action": "balance", "account": "A1001"})
    print(f"Server Alive Check: {alive_check}")
    print("\n==========================================================")
    print(" All Automated Tests Completed Successfully!")
    print("==========================================================")


class FaultTolerantBankClient:
    def __init__(self, server_a: tuple, server_b: tuple):
        self.servers = [server_a, server_b]
        self.active_idx = 0

    def send_tx(self, action: str, account: str, amount: int = 0):
        tx_id = f"tx-{uuid.uuid4().hex[:8]}"
        payload = {"tx_id": tx_id, "action": action, "account": account, "amount": amount}

        for _ in range(6):
            target_host, target_port = self.servers[self.active_idx]
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2.0)
                    s.connect((target_host, target_port))
                    s.sendall((json.dumps(payload) + "\n").encode("utf-8"))
                    raw = s.recv(4096).decode("utf-8").strip()
                    resp = json.loads(raw)

                    if resp.get("error_code") in {"NOT_PRIMARY_NODE", "QUORUM_LOST"}:
                        print(f"  [Failover] Node {target_host}:{target_port} is {resp.get('current_role')}. Switching server...")
                        self.active_idx = 1 - self.active_idx
                        time.sleep(1.0)
                        continue
                    return resp
            except Exception as e:
                print(f"  [Failover] Connection failed to {target_host}:{target_port} ({e}). Switching to backup...")
                self.active_idx = 1 - self.active_idx
                time.sleep(1.2)

        return {"status": "error", "message": "All cluster nodes unreachable"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified Mini Banking Client (Core & Cluster Bonus)")
    parser.add_argument("--mode", choices=["demo", "single", "cluster"], default="demo",
                        help="demo: Run automated core tests | single: Send 1 request | cluster: Run bonus failover stream")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Server VM IP (for demo/single modes)")
    parser.add_argument("--port", type=int, default=5000, help="Server Port (for demo/single modes)")
    parser.add_argument("--action", type=str, default="balance", help="balance | deposit | withdraw | transfer")
    parser.add_argument("--account", type=str, default="A1001", help="Account ID")
    parser.add_argument("--amount", type=int, help="Amount for deposit/withdraw/transfer")
    parser.add_argument("--server-a", type=str, help="IP:Port of Server A (e.g., 192.168.154.101:6000)")
    parser.add_argument("--server-b", type=str, help="IP:Port of Server B (e.g., 192.168.154.102:6000)")
    args = parser.parse_args()

    if args.mode == "demo":
        run_full_demo_suite(args.host, args.port)

    elif args.mode == "single":
        req = {"action": args.action, "account": args.account}
        if args.amount is not None:
            req["amount"] = args.amount
        print(json.dumps(send_request(args.host, args.port, req), indent=2))

    elif args.mode == "cluster":
        if not args.server_a or not args.server_b:
             parser.error("--server-a and --server-b are required in 'cluster' mode")
        host_a, port_a = args.server_a.split(":")
        host_b, port_b = args.server_b.split(":")
        cluster_client = FaultTolerantBankClient((host_a, int(port_a)), (host_b, int(port_b)))

        print("Starting continuous cluster transaction stream (Press Ctrl+C to stop)...")
        step = 1
        try:
            while True:
                res = cluster_client.send_tx("deposit", args.account, 10)
                print(f"[Step {step:02d}] Deposit +10 on {args.account} -> {res}")
                step += 1
                time.sleep(1.5)
        except KeyboardInterrupt:
            print("\nStopped cluster client stream.")

