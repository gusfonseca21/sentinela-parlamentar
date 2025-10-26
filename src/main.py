from flows.pipeline import pipeline
from datetime import datetime

if __name__ == "__main__":
    pipeline.serve( # type: ignore
        name="deploy-1"
    )