# Session Data Comparator

We have data in ClickHouse stored in rows each with session id, window id, and snapshot_data

We have data stored in S3. It is in folders named by session id and has lines each with window id and snapshot_data

The data should be the same but we don't know it is

This utility will

1. download the data from S3
2. decompress it and store it in a sqlite db
3. for each row use the PostHog API to get the session data, and compare it with the data in the sqlite db
4. if it is different it will store a report of the differences in the sqlite db

## Usage

e.g.

```
pnpm build && AWS_PROFILE=dev node index.js --team 16 --bucket posthog-cloud-dev-us-east-1-app-assets --skip-s3 --skip-api
```
