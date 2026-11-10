from concurrent.futures import ThreadPoolExecutor
from threading import Event
from time import monotonic, sleep

from backend.services.market_query_gate import MarketQueryGate


def test_pages_take_priority_over_background_scans():
    gate = MarketQueryGate()
    held, release = Event(), Event()
    order = []

    def first():
        with gate.acquire(page=True):
            held.set()
            assert release.wait(3)

    def run(label, page):
        with gate.acquire(page=page):
            order.append(label)

    with ThreadPoolExecutor(max_workers=3) as pool:
        a = pool.submit(first)
        assert held.wait(2)
        b = pool.submit(run, "background", False)
        c = pool.submit(run, "page", True)
        deadline = monotonic() + 2
        while not gate._waiting_pages and monotonic() < deadline:
            sleep(.005)
        assert gate._waiting_pages == 1
        release.set()
        a.result(); b.result(); c.result()
    assert order == ["page", "background"]
