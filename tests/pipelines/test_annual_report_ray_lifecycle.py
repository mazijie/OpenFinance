import sys
import types

import pytest

from backend import ray_annual_reports as job


@pytest.mark.parametrize("parse_fails", [False, True])
def test_gpu_released_before_waiting_for_silver(monkeypatch, parse_fails):
    events = []

    class Remote:
        def __init__(self, obj):
            self.obj = obj

        def options(self, **kwargs):
            return self

        def remote(self, *args):
            if isinstance(self.obj, type):
                class Actor:
                    parse = types.SimpleNamespace(remote=lambda *a: ("parse", a))
                return Actor()
            return (self.obj.__name__, args)

    def remote(obj=None, **kwargs):
        return Remote(obj) if obj is not None else lambda cls: Remote(cls)

    def get(ref):
        name, args = ref
        if name == "preflight":
            return {"manifest_path": args[0], "needs_gpu": True}
        if name == "parse":
            events.append("parse")
            if parse_fails:
                raise RuntimeError("parse failed")
            return {"manifest_path": args[0]}
        if name == "materialize":
            events.append("silver_wait")
            assert "kill" in events
            return {}
        raise AssertionError(name)

    ray = types.SimpleNamespace(remote=remote, get=get, init=lambda **kw: None,
                                cluster_resources=lambda: {"GPU": 1, job.settings.RAY_GPU_RESOURCE_LABEL: 1},
                                kill=lambda actor: events.append("kill"), shutdown=lambda: events.append("shutdown"))
    monkeypatch.setitem(sys.modules, "ray", ray)
    result = job.run_job(["report"])
    assert events.count("kill") == 1
    assert result["status"] == ("partial" if parse_fails else "succeeded")
    assert events.index("kill") < events.index("shutdown")
