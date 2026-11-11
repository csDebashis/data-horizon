def test_processor_owns_its_entrypoint_without_operations_import(monkeypatch):
    from data_horizon import processor

    calls = []
    monkeypatch.setattr(processor, "run_processor", lambda **kwargs: calls.append(kwargs))
    processor.main(["--once"])
    assert calls == [{"once": True}]


def test_processor_backfill_and_graph_use_processor_owned_jobs(isolated_data_dir, monkeypatch):
    from data_horizon import processor, processor_jobs

    calls = []
    monkeypatch.setattr(processor_jobs, "run_backfill", lambda args, settings, store: calls.append(("backfill", args.input, store.mode)))
    monkeypatch.setattr(processor_jobs, "run_graph", lambda args, settings, store: calls.append(("graph", None, store.mode)))
    processor.main(["backfill", "--input", "fixture.jsonl"])
    processor.main(["graph"])
    assert calls == [("backfill", "fixture.jsonl", "synthetic"), ("graph", None, "synthetic")]
