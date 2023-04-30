import { getSessionIdsThatNeedAPIData, insertAPIData } from "./db";

export const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

const apiToken = process.env.API_TOKEN;
if (!apiToken) {
  throw new Error("API_TOKEN environment variable not set");
}

if (!process.env.TEAM_ID || isNaN(parseInt(process.env.TEAM_ID))) {
  throw new Error("TEAM_ID environment variable not set");
}
const teamId = parseInt(process.env.TEAM_ID);

async function fetchSessionSnapshots(sessionId: string): Promise<any> {
  const url = `https://app.posthog.com/api/projects/${teamId}/session_recordings/${sessionId}/snapshots/?limit=500`;
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
      "waiting 1 second before loading a page of snapshots for session: ",
      nextPageUrl
    );
    await sleep(1000);

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

export async function loadFromAPI() {
  const sessionIds = await getSessionIdsThatNeedAPIData();

  for (const sessionId of sessionIds) {
    const snapshots = await fetchSessionSnapshots(sessionId);
    await insertAPIData(sessionId, JSON.stringify(snapshots));
  }
}
