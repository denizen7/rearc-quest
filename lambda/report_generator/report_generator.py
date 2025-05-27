import pandas as pd
import boto3
from io import StringIO
import json
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        logger.info("Received SQS event: %s", json.dumps(event))

        # S3 bucket and file paths
        bucket_name = os.environ["BUCKET_NAME"]
        bls_key = "bls/files/pr.data.0.Current"
        population_key = "datausa/population.json"

        s3_client = boto3.client('s3', region_name='us-east-1')

        # Load BLS CSV
        bls_response = s3_client.get_object(Bucket=bucket_name, Key=bls_key)
        bls_data = bls_response['Body'].read().decode('utf-8')
        bls_df = pd.read_csv(StringIO(bls_data), sep='\t')
        bls_df.columns = [col.strip().lower().replace(' ', '_') for col in bls_df.columns]
        for col in bls_df.select_dtypes(include=['object']).columns:
            bls_df[col] = bls_df[col].str.strip()

        # Load Population JSON
        pop_response = s3_client.get_object(Bucket=bucket_name, Key=population_key)
        pop_data = pop_response['Body'].read().decode('utf-8')
        json_data = json.loads(pop_data)
        population_df = pd.DataFrame(json_data["data"])
        population_df["Year"] = pd.to_numeric(population_df["Year"], errors="coerce").astype('Int64')
        population_df["Population"] = pd.to_numeric(population_df["Population"], errors="coerce").astype('Int64')
        population_df.columns = [col.strip().lower().replace(' ', '_') for col in population_df.columns]
        for col in population_df.select_dtypes(include=['object']).columns:
            population_df[col] = population_df[col].str.strip()

        # Task 1: Population stats (2013–2018)
        pop_filtered = population_df[(population_df["year"] >= 2013) & (population_df["year"] <= 2018)]
        pop_mean = round(pop_filtered["population"].mean(), 2)
        pop_std = round(pop_filtered["population"].std(ddof=0), 2)  # Population std dev
        logger.info("Population Summary (2013–2018): Mean: %.2f, Std Dev: %.2f", pop_mean, pop_std)

        # Task 2: Best year per series_id
        bls_df["value"] = pd.to_numeric(bls_df["value"], errors="coerce")
        bls_df["year"] = pd.to_numeric(bls_df["year"], errors="coerce")
        yearly_sum = bls_df.groupby(["series_id", "year"])["value"].sum().reset_index()
        best_years = (
            yearly_sum.sort_values(["series_id", "value"], ascending=[True, False])
            .drop_duplicates("series_id")
            .rename(columns={"value": "yearly_sum"})
            .reset_index(drop=True)
        )
        best_years["yearly_sum"] = best_years["yearly_sum"].round(2)
        logger.info("Best Year per Series ID:\n%s", best_years.to_string(index=False))

        # Task 3: PRS30006032 Q01 + population join
        target = bls_df[(bls_df["series_id"] == "PRS30006032") & (bls_df["period"] == "Q01")].copy()
        target = target.merge(population_df, how="inner", on="year")
        final_result = target[["series_id", "year", "period", "value", "population"]].copy()
        final_result.loc[:, "value"] = final_result["value"].round(2)
        final_result.loc[:, "population"] = final_result["population"].round(2)
        logger.info("PRS30006032 Q01 + Population:\n%s", final_result.to_string(index=False))

        return {"statusCode": 200, "body": "Report generated successfully."}

    except Exception as e:
        logger.error("Error in report generation: %s", str(e), exc_info=True)
        return {"statusCode": 500, "body": str(e)}