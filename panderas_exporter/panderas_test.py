import pandas as pd
import pandera.pandas as pa
from prometheus_client import Gauge, start_http_server, REGISTRY
import json
from time import sleep
from errors_processing import save_metrics, load_metrics

class Schema(pa.DataFrameModel):
    column1: int = pa.Field(ge=0, unique=True)
    column2: float = pa.Field(lt=10)
    column3: str = pa.Field(isin=[*"abc"], nullable=True)

    @pa.check("column3")
    def custom_check(cls, series: pd.Series) -> pd.Series:
        return series.dropna().str.len() == 1

    class Config:
        strict = True


# https://prometheus.github.io/client_python/instrumenting/gauge/
panderas_errors = Gauge(
    "panderas_error",
    "Number of errors in dataframe",
["schema", "column", "check"]  # label dimensions
)


df = pd.DataFrame({
        "column1": [1, 2, 2],          # duplicate
        "column2": [1.1, 1.2, 11],     # invalid
        "column3": ["a", "b", "c"],
        "column4": ["extra", "col", "here"]
    })
#prometheus_client.metrics.Gauge

def check_errors(dataframe):

    try:
        Schema.validate(dataframe, lazy=True)
        print("Schema is valid")
    except pa.errors.SchemaErrors as exc:
        print("Schema is invalid, Updating prometheus metrics...")
        exc_json = json.loads(str(exc))

        # 5️⃣ Log each error into Prometheus Counter
        failure_df = exc.failure_cases
        print(failure_df)
        for _, row in failure_df.iterrows():
            panderas_errors.labels(
                schema=row.get("schema", "unknown"),
                column=row.get("column", "unknown"),
                check=row.get("check", "unknown")
            ).inc()





if __name__ == "__main__":
    # https://prometheus.github.io/client_python/
    # load_metrics(prometheus_metric = panderas_errors,
    #              filename="panderas_errors.json")
    start_http_server(8000)

    # Simulate periodic validation (for demo)
    count = 1
    while count <= 100:
        check_errors(df)
        # save_metrics(metric_name="panderas_errors",
        #              filename="panderas_errors.json")
        sleep(10)
        count += 1

