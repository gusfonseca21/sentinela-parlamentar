from flows.pipeline import pipeline

if __name__ == "__main__":
    # Type ignore é utilizado pois o verificado de tipagem do Python encara pipeline como uma função "plana", mas ela possui o método serve pois é uma função decorada com @flow
    pipeline.serve(  # type: ignore
        name="deploy-1"
    )
