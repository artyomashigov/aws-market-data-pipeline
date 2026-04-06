import json
import os
from datetime import datetime, timezone

import boto3
import urllib3

# create aws clients
s3 = boto3.client("s3")
http = urllib3.PoolManager()

# read settings from lambda environment variables
BUCKET_NAME = os.environ["BUCKET_NAME"]
API_KEY = os.environ["FMP_API_KEY"]
SYMBOLS = [s.strip() for s in os.environ["SYMBOLS"].split(",") if s.strip()]

# endpoints
PRICES_URL_TEMPLATE = "https://financialmodelingprep.com/stable/historical-price-eod/full"
QUOTE_URL_TEMPLATE = "https://financialmodelingprep.com/stable/quote"
PROFILE_URL_TEMPLATE = "https://financialmodelingprep.com/stable/profile"


def fetch_json(url: str):
    # call api and parse json response
    response = http.request("GET", url)

    if response.status != 200:
        raise RuntimeError(
            f"api request failed with status {response.status}: {response.data.decode('utf-8')}"
        )

    return json.loads(response.data.decode("utf-8"))


def upload_json_to_s3(bucket: str, key: str, payload: dict):
    # upload json payload to s3
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )


def lambda_handler(event, context):
    # use utc so partition dates are consistent
    run_ts = datetime.now(timezone.utc)
    extract_date = run_ts.strftime("%Y-%m-%d")
    extracted_at = run_ts.isoformat()

    results = []

    for symbol in SYMBOLS:
        symbol_result = {"symbol": symbol}

        # fetch historical prices
        prices_url = f"{PRICES_URL_TEMPLATE}?symbol={symbol}&apikey={API_KEY}"
        try:
            prices_data = fetch_json(prices_url)

            prices_payload = {
                "symbol": symbol,
                "dataset": "prices",
                "extracted_at": extracted_at,
                "source": "financial_modeling_prep",
                "data": prices_data,
            }

            prices_key = f"raw/prices/extract_date={extract_date}/symbol={symbol}/prices.json"
            upload_json_to_s3(BUCKET_NAME, prices_key, prices_payload)
            symbol_result["prices_key"] = prices_key
            symbol_result["prices_status"] = "success"
        except Exception as e:
            symbol_result["prices_status"] = "failed"
            symbol_result["prices_error"] = str(e)

        # fetch latest quote
        quote_url = f"{QUOTE_URL_TEMPLATE}?symbol={symbol}&apikey={API_KEY}"
        try:
            quote_data = fetch_json(quote_url)

            quote_payload = {
                "symbol": symbol,
                "dataset": "quote",
                "extracted_at": extracted_at,
                "source": "financial_modeling_prep",
                "data": quote_data,
            }

            quote_key = f"raw/quotes/extract_date={extract_date}/symbol={symbol}/quote.json"
            upload_json_to_s3(BUCKET_NAME, quote_key, quote_payload)
            symbol_result["quote_key"] = quote_key
            symbol_result["quote_status"] = "success"
        except Exception as e:
            symbol_result["quote_status"] = "failed"
            symbol_result["quote_error"] = str(e)

        # fetch company profile
        profile_url = f"{PROFILE_URL_TEMPLATE}?symbol={symbol}&apikey={API_KEY}"
        try:
            profile_data = fetch_json(profile_url)

            profile_payload = {
                "symbol": symbol,
                "dataset": "profile",
                "extracted_at": extracted_at,
                "source": "financial_modeling_prep",
                "data": profile_data,
            }

            profile_key = f"raw/profiles/extract_date={extract_date}/symbol={symbol}/profile.json"
            upload_json_to_s3(BUCKET_NAME, profile_key, profile_payload)
            symbol_result["profile_key"] = profile_key
            symbol_result["profile_status"] = "success"
        except Exception as e:
            symbol_result["profile_status"] = "failed"
            symbol_result["profile_error"] = str(e)

        results.append(symbol_result)

    return {
        "statusCode": 200,
        "message": "raw ingestion completed",
        "results": results,
    }
