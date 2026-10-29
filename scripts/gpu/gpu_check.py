import json
import os
import platform
import sys
import tempfile
import time
import traceback
from pathlib import Path

MODEL = "system.ai.llama_v3_2_3b_instruct"
CATALOGS = ("pitlake_dev", "pitlake_prod")
DISTRIBUTIONS = (
    "torch",
    "transformers",
    "peft",
    "accelerate",
    "mlflow",
    "mlflow-skinny",
    "databricks-sdk",
    "pitlake",
)


def step(name, report, fn):
    started = time.monotonic()
    try:
        report[name] = fn()
        print(f"OK   {name}: {json.dumps(report[name], default=str)[:600]}", flush=True)
    except Exception as e:
        report[name] = {"error": f"{type(e).__name__}: {e}"}
        print(f"FAIL {name}: {report[name]['error'][:600]}", flush=True)
        print(traceback.format_exc(), flush=True)
    report.setdefault("seconds", {})[name] = round(time.monotonic() - started, 1)


def versions():
    from importlib import metadata

    found = {"python": platform.python_version()}
    for name in DISTRIBUTIONS:
        try:
            found[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            found[name] = "missing"
    return found


def environment():
    words = ("DATABRICKS", "JOB", "TASK", "MLFLOW")
    keys = sorted(k for k in os.environ if any(w in k for w in words))
    return {"variables": keys, "cwd": os.getcwd(), "argv": sys.argv}


def gpu():
    import torch

    if not torch.cuda.is_available():
        raise RuntimeError("torch sees no CUDA device")
    props = torch.cuda.get_device_properties(0)
    return {
        "device": props.name,
        "memory_gb": round(props.total_memory / 2**30, 1),
        "bf16": torch.cuda.is_bf16_supported(),
    }


def identity():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient().current_user.me().user_name


def latest_version(name):
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    for _ in range(12):
        found = [int(v.version) for v in client.model_versions.list(name)]
        if found:
            return max(found)
        time.sleep(10)
    raise RuntimeError(f"{name} has no versions")


def download(folder):
    import mlflow

    mlflow.set_registry_uri("databricks-uc")
    latest = latest_version(MODEL)
    path = Path(mlflow.artifacts.download_artifacts(f"models:/{MODEL}/{latest}", dst_path=folder))
    files = sorted(
        (str(p.relative_to(path)), round(p.stat().st_size / 2**20))
        for p in path.rglob("*")
        if p.is_file()
    )
    configs = [p.parent for p in path.rglob("config.json") if "model_type" in p.read_text()]
    tokenizers = [p.parent for p in path.rglob("tokenizer_config.json")]
    if not configs or not tokenizers:
        raise RuntimeError(f"no model or tokenizer directory in {files}")
    return {
        "version": latest,
        "model_dir": str(configs[0]),
        "tokenizer_dir": str(tokenizers[0]),
        "files": files[:40],
    }


def generate_and_train(found):
    import torch
    from peft import LoraConfig, get_peft_model
    from transformers import AutoModelForCausalLM, AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(found["tokenizer_dir"])
    model = AutoModelForCausalLM.from_pretrained(found["model_dir"], dtype=torch.bfloat16).to(
        "cuda"
    )
    messages = [
        {"role": "system", "content": "Answer from the excerpt and cite it as [1]."},
        {
            "role": "user",
            "content": "Excerpts:\n\n[1] Total net sales were $85,777 million for the three "
            "months ended June 29, 2024.\n\nQuestion: What were Apple's total net sales for the "
            "quarter ended June 29, 2024?",
        },
    ]
    prompt = tokenizer.apply_chat_template(
        messages, add_generation_prompt=True, return_dict=True, return_tensors="pt"
    )["input_ids"].to("cuda")
    output = model.generate(prompt, max_new_tokens=40, do_sample=False)
    answer = tokenizer.decode(output[0, prompt.shape[1] :], skip_special_tokens=True)

    model.gradient_checkpointing_enable()
    model.enable_input_require_grads()
    lora = get_peft_model(
        model,
        LoraConfig(r=16, lora_alpha=32, lora_dropout=0.05, target_modules="all-linear"),
    )
    trainable = sum(p.numel() for p in lora.parameters() if p.requires_grad)
    optimizer = torch.optim.AdamW([p for p in lora.parameters() if p.requires_grad], lr=2e-4)
    target = tokenizer(
        " Apple's total net sales were $85,777 million. [1]",
        return_tensors="pt",
        add_special_tokens=False,
    )
    ids = torch.cat([prompt, target.input_ids.to("cuda")], dim=1).repeat(1, 8)
    labels = ids.clone()
    labels[:, : prompt.shape[1]] = -100
    torch.cuda.reset_peak_memory_stats()
    losses = []
    for _ in range(5):
        loss = lora(input_ids=ids, labels=labels).loss
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()
        losses.append(round(loss.item(), 4))
    return {
        "answer": answer,
        "trainable_parameters": trainable,
        "tokens_per_step": ids.shape[1],
        "losses": losses,
        "peak_memory_gb": round(torch.cuda.max_memory_allocated() / 2**30, 2),
    }


def volumes():
    found = {}
    for catalog in CATALOGS:
        path = Path(f"/Volumes/{catalog}/gold/staging/gpu_check.json")
        try:
            path.write_text(json.dumps({"at": time.time()}))
            json.loads(path.read_text())
            path.unlink()
            found[catalog] = "read and write"
        except Exception as e:
            found[catalog] = f"{type(e).__name__}: {e}"
    return found


def main():
    report = {}
    step("versions", report, versions)
    step("environment", report, environment)
    step("gpu", report, gpu)
    step("identity", report, identity)
    step("volumes", report, volumes)
    with tempfile.TemporaryDirectory() as folder:
        step("model", report, lambda: download(folder))
        if "error" not in report["model"]:
            step("lora", report, lambda: generate_and_train(report["model"]))
    failed = [k for k, v in report.items() if isinstance(v, dict) and "error" in v]
    if failed:
        raise SystemExit(f"FAILED: {', '.join(failed)}")
    print("OK: serverless GPU is ready for the generator fine-tune")


if __name__ == "__main__":
    main()
