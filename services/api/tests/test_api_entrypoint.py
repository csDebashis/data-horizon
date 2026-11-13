def test_api_owns_its_server_entrypoint(monkeypatch):
    from data_horizon.api import main

    calls = []
    monkeypatch.setattr("uvicorn.run", lambda *args, **kwargs: calls.append((args, kwargs)))
    main(["--host", "127.0.0.1", "--port", "18000"])
    assert calls == [(("data_horizon.api:app",), {"host": "127.0.0.1", "port": 18000})]
