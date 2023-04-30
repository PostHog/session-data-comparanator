import { Readable } from "stream";
import { createGunzip } from "zlib";

export function gunzipToString(input: Readable): Promise<string> {
  return new Promise((resolve, reject) => {
    const gzip = createGunzip();

    let output = "";

    gzip.on("data", (chunk) => {
      output += chunk.toString();
    });

    gzip.on("end", () => {
      resolve(output);
    });

    gzip.on("error", (err) => {
      reject(err);
    });

    input.pipe(gzip);
  });
}
