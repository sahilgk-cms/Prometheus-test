from prometheus_client.exposition import generate_latest
from prometheus_client import REGISTRY
import prometheus_client
from prometheus_client.parser import text_string_to_metric_families
import json

# https://betterstack.com/community/guides/monitoring/prometheus-python-metrics/?utm_source=chatgpt.com
# ---- SAVE METRICS ----
def save_metrics(metric_name: str,
        filename: str):
    data = {}
    for family in generate_latest(REGISTRY).decode("utf-8").splitlines():
        if family.startswith(metric_name):
            data.setdefault("metrics_text", "")
            data["metrics_text"] += family + "\n"

    with open(filename, "w") as f:
        json.dump(data, f)
    print(f"Metrics saved to {filename}")


# ---- LOAD METRICS ----
def load_metrics(prometheus_metric: prometheus_client.metrics.Gauge,
                filename: str):
    try:
        with open(filename) as f:
            data = json.load(f)
        if "metrics_text" in data:
            for family in text_string_to_metric_families(data["metrics_text"]):
                for sample in family.samples:
                    if sample.name == "panderas_errors":
                        labels = sample.labels
                        value = sample.value
                        prometheus_metric.labels(**labels)._value.set(value)
        print("✅ Metrics loaded from file")
    except FileNotFoundError:
        print("No previous metrics found, starting fresh")