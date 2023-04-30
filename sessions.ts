import { Config } from ".";
import { pageSessions } from "./db";
import { diff } from "deep-object-diff";

export async function compareSessions(config: Config) {
  if (config["skip-compare"]) {
    console.log("Skipping compare");
    return;
  }
  // the sessions table in the database should now have both the session data from S3 and the API
  // the S3 data is jsonl, {"windowId": "blah", "data": "stringified json array"}
  // the API data is an object of windowId to json snapshot array
  // loading each line of S3 data into a map of windowId to json snapshot array
  // makes it comparable to the API data
  // any session where the two don't match we'd need to investigate
  // we want to see a count of matches to not matches

  let limit: number | undefined = 100;
  let offset: number | undefined = 0;

  const overall = { matched: 0, notMatched: 0 };

  while (!!limit) {
    // load all the session data from the database
    const page = await pageSessions(limit, offset);
    limit = page.limit;
    offset = page.offset;

    try {
      for (const session of page.sessionData) {
        const sessionId: string = session.session_id;
        const mappedS3Data: Record<string, any> = {};
        console.log("processing: " + sessionId);

        session.data
          .split("\n")
          .filter((l: string) => l.trim().length > 0)
          .map((l: string) => JSON.parse(l.trim()))
          .forEach((d: { window_id: string; data: string }) => {
            if (!mappedS3Data[d.window_id]) {
              mappedS3Data[d.window_id] = [];
            }
            const parsedData: any[] = JSON.parse(d.data);
            mappedS3Data[d.window_id] =
              mappedS3Data[d.window_id].concat(parsedData);
          });

        const apiData = JSON.parse(session.api_data);

        if (Object.keys(mappedS3Data).length === Object.keys(apiData).length) {
          console.log(
            "same number of windowIds (" + Object.keys(apiData).length + ")"
          );
          for (const windowId of Object.keys(mappedS3Data)) {
            if (mappedS3Data[windowId].length === apiData[windowId].length) {
              console.log(
                "same number of snapshots for window id (" +
                  apiData[windowId].length +
                  ")"
              );
              overall.matched++;
            } else {
              console.error(
                "different number of snapshots for window id - ruh roh - (s3: " +
                  mappedS3Data[windowId].length +
                  " vs api: " +
                  apiData[windowId].length +
                  ")"
              );
              overall.notMatched++;
            }
          }
        } else {
          console.error(
            "different number of windowIds - ruh roh - (s3: " +
              Object.keys(mappedS3Data).length +
              " vs api: " +
              Object.keys(apiData).length +
              ")"
          );
        }

        console.log("--------------------");
      }
    } catch (error: any) {
      throw error;
    }
    console.log(
      "out of " + (overall.matched + overall.notMatched) + " checked: ",
      overall
    );
  }
}
