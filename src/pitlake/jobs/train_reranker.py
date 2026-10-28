import json
import os
import random
import shutil
import tempfile
import time
from datetime import datetime
from pathlib import Path

from pitlake.config import STAGING_VOLUME, volume_path
from pitlake.generator import ADAPTER_DIR, BASE, LORA, MANIFEST, TRAINING, read_jsonl
from pitlake.jobs.train_generator import download_model, job_context, prompt_ids
from pitlake.questions import TEST, TRAIN, VALIDATION
from pitlake.reranker import (
    GROUP,
    NO,
    RERANK_MODEL,
    RERANKINGS,
    YES,
    answerable,
    judge_messages,
    reorder,
    rerank_metrics,
    reranker_folder,
    training_group,
    write_rerankings,
)
from pitlake.retrieval import TOP_K

DEFAULTS = {"epochs": "2", "learning_rate": "0.0001", "lora_rank": "16"}
ACCUMULATE = 4
PATIENCE = 1
MAX_PASSAGE_TOKENS = 768
SCORE_BATCH = 10
SEED = 0
METRIC = f"mrr_at_{TOP_K}"


def word_ids(tokenizer, word: str) -> list[int]:
    found = []
    for variant in (word, word.capitalize()):
        ids = tokenizer(variant, add_special_tokens=False)["input_ids"]
        if len(ids) != 1:
            raise RuntimeError(f"{variant!r} is not a single token")
        found.append(ids[0])
    return found


def encoder(tokenizer):
    def encode(row: dict, candidate: dict) -> list[int]:
        ids = tokenizer(candidate["text"], add_special_tokens=False)["input_ids"]
        text = tokenizer.decode(ids[:MAX_PASSAGE_TOKENS])
        at = datetime.fromisoformat(row["as_of"])
        messages = judge_messages(row["text"], at, candidate["context"], text)
        return prompt_ids(tokenizer, {"messages": messages, "as_of": row["as_of"]})

    return encode


def scores(model, prompts: list[list[int]], yes: list[int], no: list[int], pad: int):
    import torch

    width = max(len(p) for p in prompts)
    ids = torch.tensor([[pad] * (width - len(p)) + p for p in prompts], device="cuda")
    mask = torch.tensor([[0] * (width - len(p)) + [1] * len(p) for p in prompts], device="cuda")
    # Left padding shifts positions, so they are counted from each prompt's first token.
    positions = (mask.cumsum(-1) - 1).clamp(min=0)
    logits = (
        model(input_ids=ids, attention_mask=mask, position_ids=positions, logits_to_keep=1)
        .logits[:, -1]
        .float()
    )
    return torch.logsumexp(logits[:, yes], -1) - torch.logsumexp(logits[:, no], -1)


def rerank(model, encoded: dict, rows: list[dict], yes, no, pad) -> dict[str, list[str]]:
    import torch

    model.eval()
    orders = {}
    with torch.no_grad():
        for row in rows:
            prompts = encoded[row["question_id"]]
            found = []
            for start in range(0, len(prompts), SCORE_BATCH):
                batch = prompts[start : start + SCORE_BATCH]
                found.extend(scores(model, batch, yes, no, pad).tolist())
            ids = [c["chunk_id"] for c in row["candidates"]]
            orders[row["question_id"]] = reorder(ids, found)
    return orders


def train_epoch(model, optimizer, encoded, rows, yes, no, pad, epoch: int) -> float:
    import torch
    import torch.nn.functional as F

    model.train()
    rng = random.Random(SEED + epoch)
    order = [r for r in rows if answerable(r)]
    rng.shuffle(order)
    total, steps, started = 0.0, 0, time.monotonic()
    target = torch.zeros(1, dtype=torch.long, device="cuda")
    for step, row in enumerate(order, start=1):
        group = training_group(row, rng, GROUP)
        if not group:
            continue
        prompts = [encoded[row["question_id"]][i] for i in group]
        loss = F.cross_entropy(scores(model, prompts, yes, no, pad).unsqueeze(0), target)
        (loss / ACCUMULATE).backward()
        total += loss.item()
        steps += 1
        if step % ACCUMULATE == 0 or step == len(order):
            optimizer.step()
            optimizer.zero_grad()
        if step % 50 == 0:
            print(
                f"epoch {epoch} step {step}/{len(order)} loss {total / steps:.4f} "
                f"{time.monotonic() - started:.0f}s",
                flush=True,
            )
    return total / max(steps, 1)


def main() -> None:
    import torch
    from mlflow import MlflowClient
    from peft import LoraConfig, get_peft_model, get_peft_model_state_dict
    from peft import set_peft_model_state_dict as restore
    from transformers import AutoModelForCausalLM, AutoTokenizer

    started = time.monotonic()
    job_run, catalog, params = job_context(DEFAULTS)
    folder = reranker_folder(volume_path(catalog, "gold", STAGING_VOLUME), job_run)
    manifest = json.loads((folder / MANIFEST).read_text())
    if manifest["run_id"] != job_run:
        raise RuntimeError(f"{folder} was prepared by run {manifest['run_id']}, not {job_run}")
    rows = {s: read_jsonl(folder / f"{s}.jsonl") for s in (TRAIN, VALIDATION, TEST)}
    epochs, rank = int(params["epochs"]), int(params["lora_rank"])
    learning_rate = float(params["learning_rate"])
    print(f"job run {job_run} on {catalog}: {json.dumps(params)}", flush=True)

    torch.manual_seed(SEED)
    mlflow_run = os.environ["MLFLOW_RUN_ID"]
    tracking = MlflowClient()
    with tempfile.TemporaryDirectory() as scratch:
        version, model_dir, tokenizer_dir = download_model(scratch, RERANK_MODEL)
        tokenizer = AutoTokenizer.from_pretrained(tokenizer_dir)
        model = AutoModelForCausalLM.from_pretrained(model_dir, dtype=torch.bfloat16).to("cuda")
        model.config.use_cache = False
        yes, no, pad = word_ids(tokenizer, YES), word_ids(tokenizer, NO), tokenizer.eos_token_id
        encode = encoder(tokenizer)
        encoded = {
            r["question_id"]: [encode(r, c) for c in r["candidates"]]
            for split in rows.values()
            for r in split
        }
        longest = max(len(p) for prompts in encoded.values() for p in prompts)
        print(f"{RERANK_MODEL} v{version}; longest prompt {longest} tokens", flush=True)

        model.gradient_checkpointing_enable()
        model.enable_input_require_grads()
        config = LoraConfig(
            r=rank,
            lora_alpha=2 * rank,
            lora_dropout=0.05,
            target_modules="all-linear",
            task_type="CAUSAL_LM",
        )
        lora = get_peft_model(model, config)
        trainable = [p for p in lora.parameters() if p.requires_grad]
        optimizer = torch.optim.AdamW(trainable, lr=learning_rate)

        def validate() -> float:
            orders = rerank(lora, encoded, rows[VALIDATION], yes, no, pad)
            return rerank_metrics(rows[VALIDATION], orders)[METRIC]

        with lora.disable_adapter():
            base_score = validate()
        history = [{"epoch": 0, f"validation_{METRIC}": base_score}]
        tracking.log_metric(mlflow_run, f"validation.{METRIC}", base_score, step=0)
        best = (
            base_score,
            0,
            {k: v.cpu().clone() for k, v in get_peft_model_state_dict(lora).items()},
        )
        print(f"epoch 0 (base) validation {METRIC} {base_score:.4f}", flush=True)
        for epoch in range(1, epochs + 1):
            train_loss = train_epoch(lora, optimizer, encoded, rows[TRAIN], yes, no, pad, epoch)
            score = validate()
            history.append(
                {"epoch": epoch, "train_loss": train_loss, f"validation_{METRIC}": score}
            )
            tracking.log_metric(mlflow_run, "train.loss", train_loss, step=epoch)
            tracking.log_metric(mlflow_run, f"validation.{METRIC}", score, step=epoch)
            print(f"epoch {epoch} train loss {train_loss:.4f} validation {METRIC} {score:.4f}")
            if score > best[0]:
                state = get_peft_model_state_dict(lora)
                best = (score, epoch, {k: v.cpu().clone() for k, v in state.items()})
            elif epoch - best[1] >= PATIENCE:
                break
        restore(lora, {k: v.to("cuda") for k, v in best[2].items()})
        print(f"kept epoch {best[1]} (validation {METRIC} {best[0]:.4f})", flush=True)

        with lora.disable_adapter():
            base_orders = rerank(lora, encoded, rows[TEST], yes, no, pad)
        lora_orders = rerank(lora, encoded, rows[TEST], yes, no, pad)
        write_rerankings(
            folder / RERANKINGS,
            [(q, BASE, ranked) for q, ranked in base_orders.items()]
            + [(q, LORA, ranked) for q, ranked in lora_orders.items()],
        )

        saved = Path(scratch) / ADAPTER_DIR
        lora.save_pretrained(saved)
        shutil.rmtree(folder / ADAPTER_DIR, ignore_errors=True)
        shutil.copytree(saved, folder / ADAPTER_DIR)

    training = {
        "base_model": RERANK_MODEL,
        "base_model_version": version,
        "config": {
            "rank": rank,
            "alpha": 2 * rank,
            "dropout": 0.05,
            "target_modules": "all-linear",
            "learning_rate": learning_rate,
            "epochs": epochs,
            "group": GROUP,
            "accumulate": ACCUMULATE,
            "patience": PATIENCE,
            "max_passage_tokens": MAX_PASSAGE_TOKENS,
            "seed": SEED,
        },
        "best_epoch": best[1],
        "history": history,
        "trainable_parameters": sum(p.numel() for p in trainable),
        "longest_prompt_tokens": longest,
        "peak_memory_gb": round(torch.cuda.max_memory_allocated() / 2**30, 2),
        "seconds": round(time.monotonic() - started),
        "mlflow_run_id": mlflow_run,
    }
    (folder / TRAINING).write_text(json.dumps(training, indent=2, sort_keys=True))
    print(json.dumps(training, sort_keys=True), flush=True)
    print(f"OK: trained and reranked {len(rows[TEST])} test questions twice", flush=True)


if __name__ == "__main__":
    main()
