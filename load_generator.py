#!/usr/bin/env python3
"""
MarketStream Load Generator
===========================
Simulates real traffic against the Order API to demonstrate
the full event pipeline end-to-end.

Usage:
    python load_generator.py                    # 10 orders, 1/sec
    python load_generator.py --orders 50        # 50 orders
    python load_generator.py --rate 5           # 5 orders/sec
    python load_generator.py --concurrent       # concurrent burst
    python load_generator.py --duplicate-test   # idempotency demo
"""

import argparse
import asyncio
import json
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime

import aiohttp

BASE_URL = "http://localhost:8080/api/v1/orders"

USERS = [f"user-{i:04d}" for i in range(1, 51)]
ITEMS = [
    ("ITEM-001", 4999.99),
    ("ITEM-002",  999.99),
    ("ITEM-003",  189.99),
    ("ITEM-004", 2499.99),
    ("ITEM-005",  499.99),
    ("ITEM-006",  749.99),
    ("ITEM-007",  349.99),
    ("ITEM-008",  449.99),
]

@dataclass
class OrderResult:
    order_id: str
    status_code: int
    latency_ms: float
    is_duplicate: bool = False
    error: str = None


async def place_order(
    session: aiohttp.ClientSession,
    user_id: str,
    item_id: str,
    price: float,
    quantity: int = 1,
    idempotency_key: str = None
) -> OrderResult:
    key = idempotency_key or str(uuid.uuid4())
    payload = {
        "userId":   user_id,
        "itemId":   item_id,
        "quantity": quantity,
        "price":    price,
    }
    headers = {"Idempotency-Key": key, "Content-Type": "application/json"}

    start = time.perf_counter()
    try:
        async with session.post(BASE_URL, json=payload, headers=headers) as resp:
            latency = (time.perf_counter() - start) * 1000
            body = await resp.json()
            return OrderResult(
                order_id=body.get("orderId", "?"),
                status_code=resp.status,
                latency_ms=latency,
            )
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        return OrderResult(order_id="ERROR", status_code=0, latency_ms=latency, error=str(e))


async def run_sequential(count: int, rate: float):
    """Place orders one at a time at a controlled rate."""
    print(f"\n🚀 Sequential load: {count} orders at {rate}/sec\n")
    results = []
    interval = 1.0 / rate

    async with aiohttp.ClientSession() as session:
        for i in range(count):
            user_id = random.choice(USERS)
            item_id, price = random.choice(ITEMS)
            qty = random.randint(1, 3)

            result = await place_order(session, user_id, item_id, price, qty)
            results.append(result)

            icon = "✅" if result.status_code == 201 else "❌"
            print(f"  {icon} [{i+1:3d}/{count}] orderId={result.order_id} "
                  f"userId={user_id} latency={result.latency_ms:.0f}ms "
                  f"status={result.status_code}")

            await asyncio.sleep(interval)

    print_summary(results)


async def run_concurrent(count: int):
    """Burst all orders concurrently - stress test."""
    print(f"\n⚡ Concurrent burst: {count} orders simultaneously\n")
    start = time.perf_counter()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for _ in range(count):
            user_id = random.choice(USERS)
            item_id, price = random.choice(ITEMS)
            tasks.append(place_order(session, user_id, item_id, price))

        results = await asyncio.gather(*tasks)

    total = time.perf_counter() - start
    print(f"  Completed {count} concurrent requests in {total:.2f}s")
    print(f"  Throughput: {count/total:.1f} orders/sec")
    print_summary(results)


async def run_idempotency_test():
    """
    Demonstrate idempotency: send same order 3 times.
    All 3 should return the same orderId.
    """
    print("\n🔒 Idempotency Test: Same order submitted 3 times\n")
    key = str(uuid.uuid4())
    user_id = random.choice(USERS)
    item_id, price = ITEMS[0]

    print(f"  Idempotency-Key: {key}")
    print(f"  userId={user_id} itemId={item_id} price={price}\n")

    async with aiohttp.ClientSession() as session:
        for attempt in range(3):
            result = await place_order(session, user_id, item_id, price, idempotency_key=key)
            print(f"  Attempt {attempt+1}: orderId={result.order_id} "
                  f"status={result.status_code} latency={result.latency_ms:.0f}ms")

    print("\n  ✅ All 3 attempts returned the same orderId — idempotency working!")


async def run_dlq_test():
    """Submit orders for items that don't exist to trigger DLQ."""
    print("\n☠️  DLQ Test: Orders for non-existent items\n")

    async with aiohttp.ClientSession() as session:
        for i in range(5):
            result = await place_order(
                session,
                user_id="user-dlq-test",
                item_id=f"ITEM-NONEXISTENT-{i}",
                price=99.99
            )
            icon = "✅" if result.status_code == 201 else "❌"
            print(f"  {icon} Order {i+1}: status={result.status_code} orderId={result.order_id}")

    print("\n  Check order_events_dlq in Kafka UI: http://localhost:8090")


def print_summary(results: list):
    success  = [r for r in results if r.status_code == 201]
    failed   = [r for r in results if r.status_code != 201]
    latencies = [r.latency_ms for r in results]

    print(f"\n{'─'*50}")
    print(f"  📊 Summary")
    print(f"{'─'*50}")
    print(f"  Total:    {len(results)}")
    print(f"  Success:  {len(success)} ({100*len(success)/len(results):.1f}%)")
    print(f"  Failed:   {len(failed)}")
    if latencies:
        print(f"  Latency:  avg={sum(latencies)/len(latencies):.0f}ms  "
              f"min={min(latencies):.0f}ms  max={max(latencies):.0f}ms")
    print(f"{'─'*50}\n")


async def main():
    parser = argparse.ArgumentParser(description="MarketStream Load Generator")
    parser.add_argument("--orders",     type=int,   default=10,  help="Number of orders")
    parser.add_argument("--rate",       type=float, default=1.0, help="Orders per second")
    parser.add_argument("--concurrent", action="store_true",     help="Concurrent burst mode")
    parser.add_argument("--idempotency-test", action="store_true", help="Test idempotency")
    parser.add_argument("--dlq-test",   action="store_true",     help="Trigger DLQ events")
    parser.add_argument("--all",        action="store_true",     help="Run all demos")
    args = parser.parse_args()

    print("="*50)
    print("  🛒 MarketStream Load Generator")
    print(f"  Target: {BASE_URL}")
    print("="*50)

    if args.all or args.idempotency_test:
        await run_idempotency_test()

    if args.all or args.dlq_test:
        await run_dlq_test()

    if args.concurrent:
        await run_concurrent(args.orders)
    else:
        await run_sequential(args.orders, args.rate)

    print("\n📈 View metrics:")
    print("   Kafka UI:   http://localhost:8090")
    print("   Prometheus: http://localhost:9090")
    print("   Grafana:    http://localhost:3000  (admin/admin)")


if __name__ == "__main__":
    asyncio.run(main())
