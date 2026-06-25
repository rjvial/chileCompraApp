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

import concurrent.futures
import json
import os
import re
import shutil
import subprocess
import time
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


def complete_json_batch(
    requests: list[tuple[str, str]],
    schema: dict,
    system: str,
    *,
    model: str = "claude-haiku-4-5",
    max_tokens: int = 1024,
    poll_seconds: int = 30,
    log=lambda _m: None,
) -> dict[str, dict]:
    """Run a batch of structured-output calls and return {custom_id: parsed_dict}.

    The L1 canonicalization workhorse (design: L1). One shared, cached `system`
    prefix (the prompt + the implicit json schema) across every request → ~0.1x
    on the prefix; the Batch API takes another 50% off all tokens. Defaults to
    Haiku 4.5 (the decided L1 tier).

    NOTE — this is SDK-only and bills API CREDITS, not the Max subscription: the
    Batch API has no Claude-CLI equivalent. Load credits / use the anthropic_sdk
    path before a bulk run. `requests` is a list of (custom_id, user_message);
    cap each call at the API's 100k-requests / 256MB batch limit (chunk upstream).
    """
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    client = get_client()
    cached_system = [{
        "type": "text", "text": system,
        "cache_control": {"type": "ephemeral", "ttl": "1h"},  # span the batch run
    }]
    output_config = {"format": {"type": "json_schema", "schema": schema}}

    batch = client.messages.batches.create(requests=[
        Request(custom_id=cid, params=MessageCreateParamsNonStreaming(
            model=model, max_tokens=max_tokens, system=cached_system,
            output_config=output_config,
            messages=[{"role": "user", "content": user}],
        ))
        for cid, user in requests
    ])
    log(f"batch {batch.id} submitted: {len(requests)} requests")

    while True:
        batch = client.messages.batches.retrieve(batch.id)
        if batch.processing_status == "ended":
            break
        log(f"  batch {batch.id}: {batch.processing_status} "
            f"(done {batch.request_counts.succeeded + batch.request_counts.errored})")
        time.sleep(poll_seconds)

    out: dict[str, dict] = {}
    for result in client.messages.batches.results(batch.id):
        if result.result.type != "succeeded":
            log(f"  {result.custom_id}: {result.result.type}")
            continue
        msg = result.result.message
        text = next((b.text for b in msg.content if b.type == "text"), None)
        if text is not None:
            out[result.custom_id] = json.loads(text)
    log(f"batch {batch.id} complete: {len(out)}/{len(requests)} parsed")
    return out


# CLI --model accepts the short aliases; map the full ids the pipeline uses.
_CLI_ALIASES = {"claude-haiku-4-5": "haiku", "claude-sonnet-4-6": "sonnet",
                "claude-opus-4-8": "opus", "claude-opus-4-7": "opus"}


def _cli_json_concurrent(requests, schema, system, *, model, max_workers,
                         on_result=None, log=lambda _m: None):
    """Run many structured calls through the Claude CLI (Max subscription) with a
    bounded thread pool — each call is a `claude -p` subprocess. No Batch API and
    no server-side prompt caching on this path (those are SDK-only), so this is
    slower and bound by the Max usage limits; keep max_workers modest.

    `on_result(custom_id, dict)` is invoked from the main thread as each call
    completes — the hook that lets callers persist incrementally, so a kill loses
    only the in-flight calls."""
    cli_model = _CLI_ALIASES.get(model, model)
    out: dict[str, dict] = {}
    done = 0

    def one(item):
        cid, user = item
        return cid, complete_json(user, schema, system=system, model=cli_model)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(one, it): it[0] for it in requests}
        for fut in concurrent.futures.as_completed(futs):
            try:
                cid, d = fut.result()
                out[cid] = d
                if on_result is not None:
                    on_result(cid, d)
            except Exception as exc:  # noqa: BLE001 - one bad call shouldn't kill the run
                log(f"  {futs[fut][:12]}: failed ({type(exc).__name__})")
            done += 1
            if done % 200 == 0:
                log(f"  ...{done}/{len(requests)} ({len(out)} ok)")
    log(f"CLI concurrent done: {len(out)}/{len(requests)} parsed")
    return out


def complete_json_many(requests, schema, system, *,
                       model: str = "claude-haiku-4-5", max_workers: int = 8,
                       poll_seconds: int = 30, on_result=None,
                       log=lambda _m: None) -> dict[str, dict]:
    """Run a batch of structured calls and return {custom_id: parsed_dict}, using
    whichever backend is configured:

      - claude_cli (DEFAULT — the Max subscription): concurrent per-call CLI
        subprocesses. No per-token cost; slower; bounded by Max usage limits.
      - anthropic_sdk (API credits): the Batch API (−50% + prompt caching).

    `on_result(custom_id, dict)`, if given, fires as each result is ready so the
    caller can persist incrementally (kill-resumable). `requests` is a list of
    (custom_id, user_message). The L1/L3 workhorse."""
    if _backend() == "anthropic_sdk":
        out = complete_json_batch(requests, schema, system, model=model,
                                  poll_seconds=poll_seconds, log=log)
        if on_result is not None:
            for cid, d in out.items():
                on_result(cid, d)
        return out
    return _cli_json_concurrent(requests, schema, system, model=model,
                                max_workers=max_workers, on_result=on_result, log=log)


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
