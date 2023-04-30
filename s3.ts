import {
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from "@aws-sdk/client-s3";
import { Readable } from "stream";
import { gunzipToString } from "./streams";

const s3 = new S3Client({});

export async function downloadBlobContent(
  bucketName: string,
  filePrefix: string
): Promise<string> {
  console.log("Downloading file: ", filePrefix);

  const objectParams = {
    Bucket: bucketName,
    Key: filePrefix,
  };

  const object = await s3.send(new GetObjectCommand(objectParams));
  const body = object.Body;
  if (body === undefined) {
    throw new Error("Body is undefined for " + filePrefix);
  }

  // in Node body _should_ always be Readable or undefined despite what its signature suggests
  return await gunzipToString(body as Readable);
}

export async function listSessions(
  bucketName: string,
  prefix: string
): Promise<string[]> {
  console.log("Downloading session folders from S3", { bucketName, prefix });

  const command = new ListObjectsV2Command({
    Bucket: bucketName,
    Prefix: prefix,
  });

  let sessionFolders: string[] = [];

  try {
    let isTruncated = true;

    let contents: { key: string; lastModified: Date }[] = [];

    while (isTruncated) {
      const listResponse = await s3.send(command);
      const { Contents, IsTruncated, NextContinuationToken } = listResponse;

      (Contents || []).forEach((c) => {
        if (c.Key && c.LastModified) {
          contents.push({ key: c.Key, lastModified: c.LastModified });
        } else {
          throw new Error(
            "Key or LastModified not found in " + JSON.stringify(c)
          );
        }
      });

      isTruncated = !!IsTruncated;
      command.input.ContinuationToken = NextContinuationToken;
    }
    // make sure they are sorted by earliest to latest
    contents.sort((a, b) => {
      return a.lastModified.getTime() - b.lastModified.getTime();
    });

    sessionFolders = contents.map((c) => c.key);
    console.log("found sessions", sessionFolders);
  } catch (err) {
    console.error(err);
  }

  if (!sessionFolders || sessionFolders.length === 0) {
    throw new Error("No session folders found");
  }
  return sessionFolders;
}
