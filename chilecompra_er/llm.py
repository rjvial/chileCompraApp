"""LLM access for the pipeline — Claude, billed to the user's Claude Max subscription.

Provider decision (2026-06-12): no AWS Bedrock. Two backends, selected via
CHILECOMPRA_LLM_BACKEND:

  - "claude_cli" (DEFAULT): headless Claude Code (`claude -p`) — the surface
    that consumes the Claude Max subscription. JSON output is requested in
    the prompt and validated here, with one retry on parse failure.
  - "anthropic_sdk": official `anthropic` SDK with true structured outputs.
    NOTE: SDK calls bill the API organization's credit balance (pay-as-you-go),
    NOT the Max subscription — an `ant auth login` token authenticates but
    still draws API credits. Use only if the org has credits loaded.

The design note gives the LLM exactly three slots, all behind this module:
Layer-2 extraction (§7), Tier-3 classification (§8), schema strawman (§3.5).
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from functools import lru_cache

DEFAULT_MODEL = "claude-opus-4-8"
_CLI_TIMEOUT = 600  # seconds per call; Opus turns can run minutes


def _backend() -> str:
    return os.getenv("CHILECOMPRA_LLM_BACKEND", "claude_cli")


# --- backend: Claude Code CLI (Max subscription) -----------------------------

@lru_cache(maxsize=1)
def _claude_exe() -> str:
    exe = shutil.which("claude") or shutil.which("claude.cmd")
    if not exe:
        raise RuntimeError("claude CLI not found on PATH — install Claude Code "
                           "or set CHILECOMPRA_LLM_BACKEND=anthropic_sdk")
    return exe


def _cli_complete(prompt: str, system: str | None, model: str) -> str:
    cmd = [_claude_exe(), "-p", "--output-format", "json", "--model", model]
    if system:
        cmd += ["--append-system-prompt", system]
    proc = subprocess.run(cmd, input=prompt, capture_output=True, text=True,
                          encoding="utf-8", timeout=_CLI_TIMEOUT)
    if proc.returncode != 0:
        raise RuntimeError(f"claude CLI failed (exit {proc.returncode}): "
                           f"{proc.stderr.strip()[:500]}")
    envelope = json.loads(proc.stdout)
    if envelope.get("is_error"):
        raise RuntimeError(f"claude CLI error result: {envelope.get('result', '')[:500]}")
    return envelope["result"]


_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def _parse_json_text(text: str) -> dict:
    text = _FENCE.sub("", text.strip()).strip()
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("no JSON object in model output")
    return json.loads(text[start:end + 1])


# --- backend: Anthropic SDK (API credits) ------------------------------------

@lru_cache(maxsize=1)
def get_client():
    import anthropic  # lazy: offline pipeline paths never need it

    return anthropic.Anthropic()


def _sdk_json(prompt: str, schema: dict, system: str | None,
              model: str, max_tokens: int, effort: str | None) -> dict:
    output_config: dict = {"format": {"type": "json_schema", "schema": schema}}
    if effort:
        output_config["effort"] = effort
    kwargs: dict = {"system": system} if system else {}
    response = get_client().messages.create(
        model=model, max_tokens=max_tokens, thinking={"type": "adaptive"},
        output_config=output_config,
        messages=[{"role": "user", "content": prompt}], **kwargs,
    )
    if response.stop_reason == "refusal":
        raise RuntimeError("model declined the request (stop_reason=refusal)")
    if response.stop_reason == "max_tokens":
        raise RuntimeError(f"output truncated at {max_tokens} tokens")
    text = next(b.text for b in response.content if b.type == "text")
    return json.loads(text)


# --- public API ---------------------------------------------------------------

def complete_json(prompt: str, schema: dict, system: str | None = None,
                  model: str = DEFAULT_MODEL, max_tokens: int = 16000,
                  effort: str | None = None) -> dict:
    """One structured call: returns a dict matching `schema`.

    On the SDK backend the schema is enforced server-side (structured
    outputs). On the CLI backend the schema is embedded in the prompt and the
    reply is parsed/validated here, with one corrective retry. Domain
    validation against the category schema stays in the caller either way.
    """
    if _backend() == "anthropic_sdk":
        return _sdk_json(prompt, schema, system, model, max_tokens, effort)

    cli_model = "opus" if model == DEFAULT_MODEL else model
    full_prompt = (
        f"{prompt}\n\n"
        "Responde UNICAMENTE con un objeto JSON valido (sin markdown, sin "
        "comentarios) que cumpla exactamente este JSON Schema:\n"
        + json.dumps(schema, ensure_ascii=False)
    )
    text = _cli_complete(full_prompt, system, cli_model)
    try:
        return _parse_json_text(text)
    except (ValueError, json.JSONDecodeError) as exc:
        retry = (f"{full_prompt}\n\nTu respuesta anterior no fue JSON valido "
                 f"({exc}). Respondela de nuevo, SOLO el objeto JSON.")
        return _parse_json_text(_cli_complete(retry, system, cli_model))


def complete_text(prompt: str, system: str | None = None,
                  model: str = DEFAULT_MODEL, max_tokens: int = 16000,
                  effort: str | None = None) -> str:
    """One free-text call."""
    if _backend() == "anthropic_sdk":
        kwargs: dict = {"system": system} if system else {}
        if effort:
            kwargs["output_config"] = {"effort": effort}
        response = get_client().messages.create(
            model=model, max_tokens=max_tokens, thinking={"type": "adaptive"},
            messages=[{"role": "user", "content": prompt}], **kwargs,
        )
        if response.stop_reason == "refusal":
            raise RuntimeError("model declined the request (stop_reason=refusal)")
        return next(b.text for b in response.content if b.type == "text")

    cli_model = "opus" if model == DEFAULT_MODEL else model
    return _cli_complete(prompt, system, cli_model)
