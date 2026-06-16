#!/usr/bin/env python3
"""Synthetic load test harness for partition pruning benchmarks."""

import argparse
import time


def main() -> None:
    parser = argparse.ArgumentParser(description="Lemonade load test harness")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--tenant", default="dev-tenant")
    parser.add_argument("--operations", type=int, default=10)
    args = parser.parse_args()

    print(f"Load test target: {args.api_url}")
    print(f"Tenant: {args.tenant}")
    print(f"Simulating {args.operations} operation discovery requests...")

    import httpx

    start = time.time()
    with httpx.Client(timeout=30.0) as client:
        for i in range(args.operations):
            resp = client.get(f"{args.api_url}/v1/operations")
            resp.raise_for_status()
            count = len(resp.json().get("operations", []))
            print(f"  Request {i + 1}: {count} operations listed")
    elapsed = time.time() - start
    print(f"Completed in {elapsed:.2f}s ({args.operations / elapsed:.1f} req/s)")


if __name__ == "__main__":
    main()
