import * as db from "./db";
import { loadFromAPI } from "./posthogAPI";
import { compareSessions } from "./sessions";
import { loadFromS3 } from "./s3-to-sqlite";

export interface Config {
  team: number;
  bucket: string;
  "skip-s3": boolean;
  apiToken: string;
  "skip-api": boolean;
  "skip-compare": boolean;
}

function validate(config: Config) {
  if (!config.apiToken && !config["skip-api"]) {
    throw new Error("apiToken is required when not skipping API");
  }

  if (!config.bucket && !config["skip-s3"]) {
    throw new Error("bucket is required when not skipping S3");
  }

  if (!config.team && !config["skip-api"]) {
    throw new Error("team is required when not skipping API");
  }

  if (!config.team && !config["skip-s3"]) {
    throw new Error("team is required when not skipping S3");
  }
}

async function main() {
  var config = require("minimist")(process.argv.slice(2)) as Config;
  validate(config);

  db.init();

  await loadFromS3(config);
  console.log("-----------------------------");

  await loadFromAPI(config);
  console.log("-----------------------------");

  await compareSessions(config);
}

main()
  .catch((err) => {
    console.error("Error transferring data:", err);
  })
  .finally(() => {
    db.close();
  });
