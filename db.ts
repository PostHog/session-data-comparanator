import * as sqlite3 from "sqlite3";

const db = new sqlite3.Database("session_data_v3.db");

export function init() {
  db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS sessions (
          session_id TEXT PRIMARY KEY,
          data TEXT
        )`);
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

export function close() {
  db.close();
}
