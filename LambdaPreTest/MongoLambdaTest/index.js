import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { Readable } from "stream";
import { MongoClient } from "mongodb";
import { performance } from "perf_hooks";

// Function to read object keys from S3
async function getObjectKeysFromS3(s3Client, s3BucketName, objectKeysCsv) {
  const getObjectParams = {
    Bucket: s3BucketName,
    Key: objectKeysCsv,
  };

  try {
    const getObjectCommand = new GetObjectCommand(getObjectParams);
    const getObjectResponse = await s3Client.send(getObjectCommand);
    const objectKeysCsvData = await getObjectResponse.Body.transformToString();
    return objectKeysCsvData.split("\n");
  } catch (err) {
    console.error(`Error reading ${objectKeysCsv} from S3:`, err);
    throw err;
  }
}

// Function to process each object
async function processObject(s3Client, s3BucketName, objectKey, videoChunks) {
  try {
    const getObjectCommand = new GetObjectCommand({
      Bucket: s3BucketName,
      Key: objectKey,
    });
    const objectData = await s3Client.send(getObjectCommand);

    const chunks = [];
    const readableStream = objectData.Body;
    if (readableStream instanceof Readable) {
      for await (const chunk of readableStream) {
        chunks.push(chunk);
      }
    }
    const buffer = Buffer.concat(chunks);

    const startRequestTime = performance.now();
    const insertResult = await videoChunks.insertOne({
      chunkId: objectKey,
      data: buffer,
    });
    const endRequestTime = performance.now();
    const requestLatency = endRequestTime - startRequestTime;

    if (insertResult.acknowledged) {
      console.log(`Request latency for ${objectKey}: ${requestLatency} ms`);
      return {
        success: true,
        bufferSize: buffer.length,
        latency: requestLatency,
      };
    } else {
      console.error(`Error inserting ${objectKey}`);
      return { success: false, bufferSize: 0, latency: null };
    }
  } catch (err) {
    console.error(`Error processing object ${objectKey}:`, err);
    return { success: false, bufferSize: 0, latency: null };
  }
}

// Function to append metrics to CSV file on S3
async function appendMetricsToS3(s3Client, s3BucketName, csvFileName, metrics) {
  const totalTimeInMinutes = (metrics.totalTime / 1000 / 60).toFixed(2);
  const totalDataSizeInGB = (
    metrics.totalDataSize /
    (1024 * 1024 * 1024)
  ).toFixed(2); // Convert MB to GB
  const averageRequestLatencyInSeconds = (
    metrics.averageRequestLatency / 1000
  ).toFixed(2);
  const throughputInMBps = (metrics.throughput / (1024 * 1024)).toFixed(2); // Convert KB/s to MB/s

  const csvData = `${totalTimeInMinutes} minutes, ${totalDataSizeInGB} GB, ${metrics.requestCount} requests, ${averageRequestLatencyInSeconds} seconds, ${throughputInMBps} MB/s\n`;

  const appendParams = {
    Bucket: s3BucketName,
    Key: csvFileName,
    Body: csvData,
    ContentType: "text/csv",
    ACL: "private",
  };

  const appendObjectCommand = new PutObjectCommand(appendParams);
  await s3Client.send(appendObjectCommand);

  console.log(`Metrics appended to s3://${s3BucketName}/${csvFileName}`);
}

export const handler = async (event, context) => {
  try {
    const s3Client = new S3Client({ region: "us-east-2" });
    const s3BucketName = process.env.S3_BUCKET_NAME;
    const csvFileName = `mongodb-metrics.csv`;

    // Connect to MongoDB
    const client = await MongoClient.connect(process.env.MONGODB_URI);
    const db = client.db();
    console.log("Connected to MongoDB");

    const videoChunks = db.collection("VideoChunk");

    const objectKeys = await getObjectKeysFromS3(
      s3Client,
      s3BucketName,
      "object-keys.csv"
    );

    let totalDataSize = 0;
    let requestCount = 0;
    let totalRequestLatency = 0;
    let filesProcessed = 0;

    const startTime = performance.now();

    for (const objectKey of objectKeys) {
      const result = await processObject(
        s3Client,
        s3BucketName,
        objectKey,
        videoChunks
      );
      filesProcessed++;
      if (result.success) {
        totalDataSize += result.bufferSize;
        requestCount++;
        totalRequestLatency += result.latency;
      }
      console.log(`Files processed: ${filesProcessed}`);
    }

    const endTime = performance.now();
    const totalTime = endTime - startTime;

    const averageRequestLatency =
      requestCount > 0 ? totalRequestLatency / requestCount : 0;
    const throughput =
      totalDataSize > 0 && totalTime > 0
        ? (totalDataSize / totalTime) * 1000
        : 0;

    await appendMetricsToS3(s3Client, s3BucketName, csvFileName, {
      totalTime,
      totalDataSize,
      requestCount,
      averageRequestLatency,
      throughput,
    });

    await client.close();

    return {
      statusCode: 200,
      body: JSON.stringify({
        totalTime,
        totalDataSize,
        requestCount,
        averageRequestLatency,
        throughput,
      }),
    };
  } catch (err) {
    console.error("Error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal Server Error" }),
    };
  }
};
