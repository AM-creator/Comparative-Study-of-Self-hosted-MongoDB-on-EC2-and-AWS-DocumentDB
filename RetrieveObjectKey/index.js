import {
  S3Client,
  ListObjectsV2Command,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";

// AWS configuration

const csvFilePath = "/tmp/object-keys.csv"; // Store the file in /tmp directory
const csvKey = "object-keys.csv"; // Key under which the CSV file will be stored in the bucket

// Create an S3 client
const s3Client = new S3Client({ region: "us-east-2" });
const s3BucketName = process.env.S3_BUCKET_NAME;

// Function to retrieve all object keys from an S3 bucket
async function getAllObjectKeys() {
  let objectKeys = [];
  let continuationToken = null;

  do {
    const params = {
      Bucket: s3BucketName,
      ContinuationToken: continuationToken,
    };

    try {
      const data = await s3Client.send(new ListObjectsV2Command(params));
      const keys = data.Contents.map((obj) => obj.Key);
      objectKeys = objectKeys.concat(keys);
      continuationToken = data.NextContinuationToken;
    } catch (err) {
      console.error("Error listing objects:", err);
      throw err;
    }
  } while (continuationToken);

  return objectKeys;
}

// Function to write object keys to a CSV file
async function writeKeysToCSV(keys) {
  const csvData = keys.join("\n");
  await fs.writeFile(csvFilePath, csvData);
  console.log(`Object keys written to ${csvFilePath}`);
}

// Function to upload CSV file to S3 bucket
async function uploadCsvToS3() {
  const fileContent = await fs.readFile(csvFilePath);

  const params = {
    Bucket: s3BucketName,
    Key: csvKey,
    Body: fileContent,
    ContentType: "text/csv", // Set content type if needed
  };

  try {
    await s3Client.send(new PutObjectCommand(params));
    console.log(`CSV file uploaded to S3 bucket: ${s3BucketName}/${csvKey}`);
  } catch (err) {
    console.error("Error uploading CSV file to S3:", err);
    throw err;
  }
}

// Main function
export async function handler(event, context) {
  try {
    const objectKeys = await getAllObjectKeys();
    await writeKeysToCSV(objectKeys);
    await uploadCsvToS3();
    return { statusCode: 200, body: "CSV file uploaded to S3 bucket" };
  } catch (err) {
    console.error("Error:", err);
    return { statusCode: 500, body: "Internal Server Error" };
  }
}
