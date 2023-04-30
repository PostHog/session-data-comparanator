import * as sqlite3 from "sqlite3";

const db = new sqlite3.Database("session_data_v3.db");

export function init() {
  db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS sessions (
          session_id TEXT PRIMARY KEY,
          data TEXT,
          api_data TEXT
        )`);
  });
}

export async function alreadyExists(sessionId: string): Promise<boolean> {
  return new Promise((resolve, reject) => {
    db.get(
      `SELECT session_id FROM sessions WHERE session_id = ?`,
      [sessionId],
      (err: any, row: any) => {
        if (err) {
          reject(err);
        } else {
          resolve(!!row?.session_id);
        }
      }
    );
  });
}

export async function getSessionIdsThatNeedAPIData(): Promise<string[]> {
  return new Promise((resolve, reject) => {
    db.all(
      `SELECT session_id FROM sessions where api_data is null`,
      (err: any, rows: any) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows.map((row: any) => row.session_id));
        }
      }
    );
  });
}

export async function mergeSessionData(sessionId: string, data: string) {
  // load any existing session data from sqllite
  const existingSessionData: string = await new Promise((resolve, reject) => {
    db.get(
      `SELECT data FROM sessions WHERE session_id = ?`,
      [sessionId],
      (err: any, row: any) => {
        if (err) {
          reject(err);
        } else {
          resolve(row?.data);
        }
      }
    );
  });
  // existingSessionData and fileContents are a jsonl string
  // i.e. it is many lines of json objects

  // merge the new data with the existing data
  const mergedData = (existingSessionData || "") + data;
  console.log(
    sessionId + " merged data has ",
    (mergedData.match(/\n/g) || "").length + 1 + " lines"
  );

  db.run(
    `INSERT OR REPLACE INTO sessions (session_id, data) VALUES (?, ?)`,
    [sessionId, mergedData],
    (err) => {
      if (err) {
        console.error(
          `Error inserting data for session ${sessionId}:`,
          err.message
        );
      } else {
        console.log(`Successfully inserted data for session ${sessionId}`);
      }
    }
  );
}

export async function insertAPIData(
  sessionId: string,
  data: string
): Promise<void> {
  console.log("inserting API data for session", sessionId);
  return new Promise((resolve, reject) => {
    db.run(
      `UPDATE sessions SET api_data = ? WHERE session_id = ?`,
      [data, sessionId],
      (err) => {
        if (err) {
          console.error(
            `Error inserting API data for session ${sessionId}:`,
            err.message
          );
          reject(err);
        } else {
          console.log(
            `Successfully inserted API data for session ${sessionId}`
          );
          resolve();
        }
      }
    );
  });
}

export function close() {
  console.log("!!!!!!!!!!!!!!!!!!!");
  console.log("calling db.close()");
  console.log("!!!!!!!!!!!!!!!!!!!");
  db.close();
}

export interface SessionDataPage {
  sessionData: any[];
  limit?: number;
  offset?: number;
}

export async function pageSessions(
  limit = 100,
  offset = 0
): Promise<SessionDataPage> {
  const sessionData: any[] = await new Promise((resolve, reject) => {
    db.all(
      `SELECT session_id, data, api_data FROM sessions where data is not null and api_data is not null limit ${
        limit + 1
      } offset ${offset}`,
      (err: any, rows: any) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      }
    );
  });
  if (sessionData.length === 101) {
    // there is more data to page through
    return {
      sessionData: sessionData.slice(0, 100),
      limit,
      offset: offset + 100,
    };
  } else {
    return {
      sessionData,
    };
  }
}
