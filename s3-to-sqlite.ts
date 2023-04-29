import {
  S3Client,
  ListObjectsCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import * as sqlite3 from "sqlite3";
import { v4 as uuidv4 } from "uuid";
import { createGunzip } from "zlib";
import { Readable, Writable } from "stream";

if (!process.env.BUCKET_NAME) {
  throw new Error("BUCKET_NAME environment variable not set");
}

if (!process.env.TEAM_ID || isNaN(parseInt(process.env.TEAM_ID))) {
  throw new Error("TEAM_ID environment variable not set");
}
const teamId = parseInt(process.env.TEAM_ID);

const s3 = new S3Client({});
const bucketName = process.env.BUCKET_NAME;
const prefix = `session_recordings/team_id/${teamId}/session_id`;

const db = new sqlite3.Database("session_data_v3.db");

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    data TEXT
  )`);
});

async function downloadS3FolderContents(
  folderPrefix: string
): Promise<string[]> {
  let files: string[] = [];

  async function downloadFiles(marker?: string): Promise<void> {
    console.log("Downloading files from S3", {
      bucketName,
      folderPrefix: folderPrefix + "data",
      marker,
    });
    const params = {
      Bucket: bucketName,
      folderPrefix: folderPrefix + "data",
      Marker: marker,
    };

    const response = await s3.send(new ListObjectsCommand(params));
    const contents = response.Contents;

    for (const content of contents || []) {
      const objectParams = {
        Bucket: bucketName,
        Key: content.Key,
      };

      const object = await s3.send(new GetObjectCommand(objectParams));
      const body = await object.Body?.transformToString();
      if (body === undefined) {
        throw new Error(
          "Body is undefined for " +
            content.Key +
            " in " +
            bucketName +
            " bucket"
        );
      }
      const unzippedData = await unzipText(body);

      files.push(unzippedData);
    }

    if (response.IsTruncated) {
      await downloadFiles(response.NextMarker);
    }
  }

  await downloadFiles();
  return files;
}

async function main() {
  const sessionFoldersResponse = await s3.send(
    new ListObjectsCommand({
      Bucket: bucketName,
      Prefix: prefix,
      Delimiter: "/",
    })
  );

  const sessionFolders = sessionFoldersResponse.CommonPrefixes;

  if (!sessionFolders || sessionFolders.length === 0) {
    console.log("No session folders found", sessionFoldersResponse);
    throw new Error("No session folders found");
  } else {
    console.log(`Found ${sessionFolders.length} session folders`);
  }

  for (const folder of sessionFolders || []) {
    if (!folder.Prefix) {
      throw new Error("Folder prefix not found");
    }

    const sessionID = uuidv4();
    const files = await downloadS3FolderContents(folder.Prefix);

    const jsonData = JSON.stringify(files);
    db.run(
      `INSERT OR REPLACE INTO sessions (session_id, data) VALUES (?, ?)`,
      [sessionID, jsonData],
      (err) => {
        if (err) {
          console.error(
            `Error inserting data for session ${sessionID}:`,
            err.message
          );
        } else {
          console.log(`Successfully inserted data for session ${sessionID}`);
        }
      }
    );
  }
}

main()
  .then(() => console.log("S3 data transferred to SQLite database"))
  .catch((err) => console.error("Error transferring data:", err))
  .finally(() => db.close());

function unzipText(s: string): Promise<string> {
  return new Promise(async (resolve, reject) => {
    const gunzipStream = createGunzip();
    const inputStream = Readable.from(s);
    const stream = inputStream.pipe(gunzipStream);
    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    const decompressedData = Buffer.concat(chunks).toString();
    resolve(decompressedData);
  });
}
