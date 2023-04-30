import { downloadBlobContent, listSessions } from "./s3";
import * as db from "./db";
import { mergeSessionData } from "./db";
import { Config } from ".";

export async function loadFromS3(config: Config) {
  if (config["skip-s3"]) {
    console.log("Skipping S3");
    return;
  }

  const prefix = `session_recordings/team_id/${config.team}/session_id/`;
  const sessionFolders = await listSessions(config.bucket, prefix);

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
      const fileContent = await downloadBlobContent(config.bucket, blobPrefix);

      await mergeSessionData(sessionIdWithBlobs, fileContent);
    }
  }
  console.log("session data loaded from S3");
}
