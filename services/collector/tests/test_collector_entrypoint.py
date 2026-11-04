def test_collector_owns_its_entrypoint(monkeypatch):
    from data_horizon import collector

    calls = []
    monkeypatch.setattr(collector, "run_collector", lambda **kwargs: calls.append(kwargs))
    collector.main(["--once"])
    assert calls == [{"once": True}]
