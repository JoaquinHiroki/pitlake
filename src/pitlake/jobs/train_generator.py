import json
import os
import random
import shutil
import tempfile
import time
from datetime import datetime
from pathlib import Path

from pitlake.config import STAGING_VOLUME, validate_identifier, validate_segment, volume_path
from pitlake.generator import (
    ADAPTER_DIR,
    BASE,
    BASE_MODEL,
    LORA,
    MANIFEST,
    PREDICTIONS,
    TRAINING,
    generator_folder,
    read_jsonl,
    write_jsonl,
)
from pitlake.questions import TEST, TRAIN, VALIDATION

DEFAULTS = {"epochs": "3", "learning_rate": "0.0001", "lora_rank": "16"}
ACCUMULATE = 8
PATIENCE = 1
MAX_NEW_TOKENS = 120
SEED = 0


def job_context(defaults: dict[str, str] = DEFAULTS) -> tuple[str, str, dict[str, str]]:
    from databricks.sdk import WorkspaceClient
    from mlflow import MlflowClient

    tags = MlflowClient().get_run(os.environ["MLFLOW_RUN_ID"]).data.tags
    job_run = tags.get("mlflow.databricks.jobRunID") or tags.get("job_run_id")
    if not job_run:
        raise RuntimeError(f"the MLflow run carries no job run ID: {sorted(tags)}")
    client = WorkspaceClient()
    run = client.jobs.get_run(int(job_run))
    catalog = ((client.jobs.get(run.job_id).settings.tags) or {}).get("catalog", "")
    params = dict(defaults)
    for p in run.job_parameters or []:
        value = p.value if p.value is not None else p.default
        if p.name in defaults and value not in (None, ""):
            params[p.name] = value
    return validate_segment(job_run), validate_identifier(catalog), params


def latest_version(client, name: str) -> int:
    for _ in range(12):
        found = [int(v.version) for v in client.model_versions.list(name)]
        if found:
            return max(found)
        time.sleep(10)
    raise RuntimeError(f"{name} has no versions")


def download_model(folder: str, name: str = BASE_MODEL) -> tuple[int, Path, Path]:
    import mlflow
    from databricks.sdk import WorkspaceClient

    mlflow.set_registry_uri("databricks-uc")
    version = latest_version(WorkspaceClient(), name)
    path = Path(mlflow.artifacts.download_artifacts(f"models:/{name}/{version}", dst_path=folder))
    configs = [p.parent for p in path.rglob("config.json") if "model_type" in p.read_text()]
    tokenizers = [p.parent for p in path.rglob("tokenizer_config.json")]
    if not configs or not tokenizers:
        raise RuntimeError(f"{name} version {version} has no model or tokenizer")
    return version, configs[0], tokenizers[0]


def prompt_ids(tokenizer, item: dict) -> list[int]:
    as_of = datetime.fromisoformat(item["as_of"])
    return tokenizer.apply_chat_template(
        item["messages"],
        add_generation_prompt=True,
        return_dict=True,
        date_string=f"{as_of:%d %b %Y}",
    )["input_ids"]


def encode(tokenizer, item: dict) -> tuple[list[int], list[int]]:
    target = tokenizer(item["target"], add_special_tokens=False)["input_ids"]
    return prompt_ids(tokenizer, item), [*target, tokenizer.eos_token_id]


def target_loss(model, prompt: list[int], target: list[int]):
    import torch
    import torch.nn.functional as F

    ids = torch.tensor([prompt + target], device="cuda")
    logits = model(input_ids=ids, logits_to_keep=len(target) + 1).logits[0, :-1].float()
    return F.cross_entropy(logits, torch.tensor(target, device="cuda"))


def mean_loss(model, encoded) -> float:
    import torch

    model.eval()
    with torch.no_grad():
        return sum(target_loss(model, p, t).item() for p, t in encoded) / len(encoded)


def train_epoch(model, optimizer, encoded, epoch: int) -> float:
    model.train()
    order = list(encoded)
    random.Random(SEED + epoch).shuffle(order)
    total, started = 0.0, time.monotonic()
    for step, (prompt, target) in enumerate(order, start=1):
        loss = target_loss(model, prompt, target)
        (loss / ACCUMULATE).backward()
        total += loss.item()
        if step % ACCUMULATE == 0 or step == len(order):
            optimizer.step()
            optimizer.zero_grad()
        if step % 50 == 0:
            print(
                f"epoch {epoch} step {step}/{len(order)} loss {total / step:.4f} "
                f"{time.monotonic() - started:.0f}s",
                flush=True,
            )
    return total / len(order)


def generate(model, tokenizer, prompt: list[int]) -> str:
    import torch

    ids = torch.tensor([prompt], device="cuda")
    with torch.no_grad():
        output = model.generate(
            input_ids=ids,
            attention_mask=torch.ones_like(ids),
            max_new_tokens=MAX_NEW_TOKENS,
            do_sample=False,
            temperature=None,
            top_p=None,
            pad_token_id=tokenizer.eos_token_id,
        )
    return tokenizer.decode(output[0, ids.shape[1] :], skip_special_tokens=True).strip()


def main() -> None:
    import torch
    from mlflow import MlflowClient
    from peft import LoraConfig, get_peft_model, get_peft_model_state_dict
    from peft import set_peft_model_state_dict as restore
    from transformers import AutoModelForCausalLM, AutoTokenizer

    started = time.monotonic()
    job_run, catalog, params = job_context()
    folder = generator_folder(volume_path(catalog, "gold", STAGING_VOLUME), job_run)
    manifest = json.loads((folder / MANIFEST).read_text())
    if manifest["run_id"] != job_run:
        raise RuntimeError(f"{folder} was prepared by run {manifest['run_id']}, not {job_run}")
    examples = {s: read_jsonl(folder / f"{s}.jsonl") for s in (TRAIN, VALIDATION, TEST)}
    epochs, rank = int(params["epochs"]), int(params["lora_rank"])
    learning_rate = float(params["learning_rate"])
    print(f"job run {job_run} on {catalog}: {json.dumps(params)}", flush=True)

    torch.manual_seed(SEED)
    mlflow_run = os.environ["MLFLOW_RUN_ID"]
    tracking = MlflowClient()
    with tempfile.TemporaryDirectory() as scratch:
        version, model_dir, tokenizer_dir = download_model(scratch)
        tokenizer = AutoTokenizer.from_pretrained(tokenizer_dir)
        model = AutoModelForCausalLM.from_pretrained(model_dir, dtype=torch.bfloat16).to("cuda")
        model.config.use_cache = False
        encoded = {s: [encode(tokenizer, item) for item in rows] for s, rows in examples.items()}
        longest = max(len(p) + len(t) for rows in encoded.values() for p, t in rows)
        print(f"{BASE_MODEL} v{version}; longest example {longest} tokens", flush=True)

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

        with lora.disable_adapter():
            base_loss = mean_loss(lora, encoded[VALIDATION])
        history = [{"epoch": 0, "validation_loss": base_loss}]
        tracking.log_metric(mlflow_run, "validation.loss", base_loss, step=0)
        best = (
            base_loss,
            0,
            {k: v.cpu().clone() for k, v in get_peft_model_state_dict(lora).items()},
        )
        print(f"epoch 0 (base) validation loss {base_loss:.4f}", flush=True)
        for epoch in range(1, epochs + 1):
            train_loss = train_epoch(lora, optimizer, encoded[TRAIN], epoch)
            validation_loss = mean_loss(lora, encoded[VALIDATION])
            history.append(
                {"epoch": epoch, "train_loss": train_loss, "validation_loss": validation_loss}
            )
            tracking.log_metric(mlflow_run, "train.loss", train_loss, step=epoch)
            tracking.log_metric(mlflow_run, "validation.loss", validation_loss, step=epoch)
            print(
                f"epoch {epoch} train loss {train_loss:.4f} validation loss {validation_loss:.4f}",
                flush=True,
            )
            if validation_loss < best[0]:
                state = get_peft_model_state_dict(lora)
                best = (validation_loss, epoch, {k: v.cpu().clone() for k, v in state.items()})
            elif epoch - best[1] >= PATIENCE:
                break
        restore(lora, {k: v.to("cuda") for k, v in best[2].items()})
        print(f"kept epoch {best[1]} (validation loss {best[0]:.4f})", flush=True)

        model.gradient_checkpointing_disable()
        model.config.use_cache = True
        lora.eval()
        predictions = []
        for name in (BASE, LORA):
            for n, (item, (prompt, _)) in enumerate(
                zip(examples[TEST], encoded[TEST], strict=True), start=1
            ):
                if name == BASE:
                    with lora.disable_adapter():
                        answer = generate(lora, tokenizer, prompt)
                else:
                    answer = generate(lora, tokenizer, prompt)
                predictions.append(
                    {"question_id": item["question_id"], "model": name, "answer": answer}
                )
                if n % 25 == 0:
                    print(f"{name}: answered {n}/{len(examples[TEST])}", flush=True)

        saved = Path(scratch) / ADAPTER_DIR
        lora.save_pretrained(saved)
        shutil.rmtree(folder / ADAPTER_DIR, ignore_errors=True)
        shutil.copytree(saved, folder / ADAPTER_DIR)

    write_jsonl(folder / PREDICTIONS, predictions)
    training = {
        "base_model": BASE_MODEL,
        "base_model_version": version,
        "config": {
            "rank": rank,
            "alpha": 2 * rank,
            "dropout": 0.05,
            "target_modules": "all-linear",
            "learning_rate": learning_rate,
            "epochs": epochs,
            "accumulate": ACCUMULATE,
            "patience": PATIENCE,
            "max_new_tokens": MAX_NEW_TOKENS,
            "seed": SEED,
        },
        "best_epoch": best[1],
        "history": history,
        "trainable_parameters": sum(p.numel() for p in trainable),
        "longest_example_tokens": longest,
        "peak_memory_gb": round(torch.cuda.max_memory_allocated() / 2**30, 2),
        "seconds": round(time.monotonic() - started),
        "mlflow_run_id": mlflow_run,
    }
    (folder / TRAINING).write_text(json.dumps(training, indent=2, sort_keys=True))
    print(json.dumps(training, sort_keys=True), flush=True)
    print(f"OK: trained and answered {len(examples[TEST])} test questions twice", flush=True)


if __name__ == "__main__":
    main()
