import { Config } from ".";
import { getSessionIdsThatNeedAPIData, insertAPIData } from "./db";

export const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

async function fetchSessionSnapshots(
  sessionId: string,
  teamId: number,
  apiToken: string,
  delay: number,
  pageSize: number
): Promise<any> {
  const url = `https://app.posthog.com/api/projects/${teamId}/session_recordings/${sessionId}/snapshots/?limit=${pageSize}`;
  const headers = new Headers({
    "Content-Type": "application/json",
    Authorization: `Bearer ${apiToken}`,
  });

  let snapshots: Record<string, Record<string, any>[]> = {};
  let nextPageUrl: string | null = url;

  while (nextPageUrl) {
    // sleep before each API call, this will make things super slow
    // but will avoid rate limiting and avoid overloading ClickHouse
    // if the script is run while we have a burst of traffic
    console.log(
      `waiting ${
        delay / 1000
      } second(s) before loading a page of snapshots for session: `,
      nextPageUrl
    );
    await sleep(delay);

    try {
      const response = await fetch(nextPageUrl, { headers });
      if (!response.ok) {
        // noinspection ExceptionCaughtLocallyJS
        throw new Error(`HTTP error! Status: ${response.status}`);
      }
      const data = await response.json();
      const snapshotsByWindowId = data.snapshot_data_by_window_id as Record<
        string,
        Record<string, any>[]
      >;
      Object.keys(snapshotsByWindowId).forEach((windowId) => {
        if (!snapshots[windowId]) {
          snapshots[windowId] = [];
        }
        snapshots[windowId].push(...snapshotsByWindowId[windowId]);
      });

      nextPageUrl = data.next;
    } catch (error) {
      console.error("Error fetching session snapshots:", error);
      throw error;
    }
  }

  return snapshots;
}

export async function loadFromAPI(config: Config) {
  if (config["skip-api"]) {
    console.log("Skipping API data load");
    return;
  }

  const sessionIds = await getSessionIdsThatNeedAPIData();

  for (const sessionId of sessionIds) {
    const snapshots = await fetchSessionSnapshots(
      sessionId,
      config.team,
      config.apiToken,
      config.delay,
      config.pageSize
    );
    await insertAPIData(sessionId, JSON.stringify(snapshots));
  }
  console.log("sessions loaded from PostHog API");
}
