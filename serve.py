from orchestration.flows.pipeline import etl_pipeline_flow

if __name__ == "__main__":
    etl_pipeline_flow.serve(
        name="daily-etl",
        cron="0 7 * * *",
    )
