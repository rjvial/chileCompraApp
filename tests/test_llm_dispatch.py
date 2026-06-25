"""complete_json_many routes to the Max/CLI concurrent path by default and to the
SDK Batch API only when the backend is switched. Monkeypatched — no subprocess,
no network."""
from __future__ import annotations

from chilecompra_er import llm


def test_many_uses_cli_concurrent_by_default(monkeypatch):
    monkeypatch.setenv("CHILECOMPRA_LLM_BACKEND", "claude_cli")
    seen = []

    def fake_complete_json(user, schema, system=None, model=None, **kw):
        seen.append((user, model))
        return {"u": user}

    monkeypatch.setattr(llm, "complete_json", fake_complete_json)
    out = llm.complete_json_many([("a", "ua"), ("b", "ub")], {}, "sys",
                                 model="claude-haiku-4-5", max_workers=2)
    assert out == {"a": {"u": "ua"}, "b": {"u": "ub"}}
    assert all(m == "haiku" for _u, m in seen)        # full id mapped to CLI alias


def test_many_oauth_uses_bare_concurrent_with_real_model_id(monkeypatch):
    monkeypatch.setenv("CHILECOMPRA_LLM_BACKEND", "claude_oauth")
    seen = []

    def fake_complete_json(user, schema, system=None, model=None, **kw):
        seen.append((user, model))
        return {"u": user}

    monkeypatch.setattr(llm, "complete_json", fake_complete_json)
    out = llm.complete_json_many([("a", "ua")], {}, "sys",
                                 model="claude-haiku-4-5", max_workers=2)
    assert out == {"a": {"u": "ua"}}
    # OAuth hits the real API → must use the REAL model id, not the CLI alias
    assert seen[0][1] == "claude-haiku-4-5"


def test_oauth_system_prepends_claude_code_identity():
    blocks = llm._oauth_system("My instructions")
    assert blocks[0]["text"] == llm.CLAUDE_CODE_IDENTITY      # required first block
    assert blocks[1]["text"] == "My instructions"
    assert blocks[1]["cache_control"]["type"] == "ephemeral"  # our system is cached


def test_many_uses_batch_when_sdk_backend(monkeypatch):
    monkeypatch.setenv("CHILECOMPRA_LLM_BACKEND", "anthropic_sdk")
    captured = {}

    def fake_batch(requests, schema, system, model=None, **kw):
        captured["model"] = model
        return {cid: {"ok": 1} for cid, _ in requests}

    monkeypatch.setattr(llm, "complete_json_batch", fake_batch)
    out = llm.complete_json_many([("x", "ux")], {}, "sys", model="claude-haiku-4-5")
    assert out == {"x": {"ok": 1}} and captured["model"] == "claude-haiku-4-5"
