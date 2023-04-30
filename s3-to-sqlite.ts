import { downloadBlobContent, listSessions } from "./s3";
import * as db from "./db";
import { mergeSessionData } from "./db";

if (!process.env.BUCKET_NAME) {
  throw new Error("BUCKET_NAME environment variable not set");
}

if (!process.env.TEAM_ID || isNaN(parseInt(process.env.TEAM_ID))) {
  throw new Error("TEAM_ID environment variable not set");
}
const teamId = parseInt(process.env.TEAM_ID);

const bucketName = process.env.BUCKET_NAME;
const prefix = `session_recordings/team_id/${teamId}/session_id/`;

async function main() {
  db.init();
  const sessionFolders = await listSessions(bucketName, prefix);
  if (!sessionFolders || sessionFolders.length === 0) {
    console.log("No session folders found");
    throw new Error("No session folders found");
  }

  const blobPrefixesBySessionId: { [key: string]: string[] } = {};

  for (const folder of sessionFolders || []) {
    console.log("Downloading folder: ", folder);
    if (!folder) {
      throw new Error("Folder prefix not found");
    }

    // get the session id from the folder name using regex session_recordings\/team_id\/\d+\/session_id\/(.*)\/data.*
    const sessionID = folder.match(
      /session_recordings\/team_id\/\d+\/session_id\/(.*)\/data.*/
    )?.[1];

    if (!sessionID) {
      throw new Error("Session ID not found in blobPrefix " + folder);
    }
    if (!blobPrefixesBySessionId[sessionID]) {
      blobPrefixesBySessionId[sessionID] = [];
    }
    blobPrefixesBySessionId[sessionID].push(folder);
  }

  for (const [sessionID, blobPrefixes] of Object.entries(
    blobPrefixesBySessionId
  )) {
    for (const blobPrefix of blobPrefixes) {
      const fileContent = await downloadBlobContent(bucketName, blobPrefix);

      await mergeSessionData(sessionID, fileContent);
    }
  }
}

main()
  .then(() => console.log("S3 data transferred to SQLite database"))
  .catch((err) => console.error("Error transferring data:", err))
  .finally(() => db.close());
