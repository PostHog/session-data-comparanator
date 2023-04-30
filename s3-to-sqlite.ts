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

  const blobPrefixesBySessionId: { [key: string]: string[] } = {};

  for (const folder of sessionFolders || []) {
    console.log("Downloading folder: ", folder);
    if (!folder) {
      throw new Error("Folder prefix not found");
    }

    const sessionId = folder.match(
      /session_recordings\/team_id\/\d+\/session_id\/(.*)\/data.*/
    )?.[1];

    if (!sessionId) {
      throw new Error("Session ID not found in blobPrefix " + folder);
    }
    if (!blobPrefixesBySessionId[sessionId]) {
      blobPrefixesBySessionId[sessionId] = [];
    }
    blobPrefixesBySessionId[sessionId].push(folder);
  }

  for (const [sessionIdWithBlobs, blobPrefixes] of Object.entries(
    blobPrefixesBySessionId
  )) {
    if (await db.alreadyExists(sessionIdWithBlobs)) {
      console.log(
        "skipping sessionId: ",
        sessionIdWithBlobs,
        " it is already in the database"
      );
      continue;
    }
    for (const blobPrefix of blobPrefixes) {
      const fileContent = await downloadBlobContent(bucketName, blobPrefix);

      await mergeSessionData(sessionIdWithBlobs, fileContent);
    }
  }
}

main()
  .then(() => console.log("S3 data transferred to SQLite database"))
  .catch((err) => console.error("Error transferring data:", err))
  .finally(() => db.close());
