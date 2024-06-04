import { MongoClient } from "mongodb";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { Readable } from "stream";
import { performance } from "perf_hooks";

export const handler = async (event, context) => {
  try {
    const startTime = performance.now();
    const s3Client = new S3Client({ region: "us-east-2" });
    const s3BucketName = event.s3BucketName;
    const csvFileName = `documentdb-metrics.csv`;
    const batchNumber = event.batchNumber;
    const objectKeysCsv = event.objectKeysCsv;
    const experimentNumber = event.experimentNumber;

    // Connect to DocumentDB
    const client = await MongoClient.connect(process.env.DOCUMENTDB_ENDPOINT, {
      tlsCAFile: `global-bundle.pem`,
    });
    const db = client.db("DocumentDBCase");
    console.log("Connected to DocumentDB");

    const videoChunks = db.collection("VideoChunk");

    const objectKeys = await readObjectKeysFromS3(
      s3Client,
      s3BucketName,
      objectKeysCsv
    );

    let totalDataSize = 0;
    let requestCount = 0;
    let totalRequestLatency = 0;

    for (const objectKey of objectKeys) {
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
        totalDataSize += buffer.length;

        const startRequestTime = performance.now();
        const insertResult = await videoChunks.insertOne({
          chunkId: objectKey,
          data: buffer,
        });
        const endRequestTime = performance.now();
        const requestLatency = endRequestTime - startRequestTime;
        totalRequestLatency += requestLatency;

        if (insertResult.acknowledged) {
          requestCount++;
          console.log(`Request latency for ${objectKey}: ${requestLatency} ms`);
        } else {
          console.error(`Error inserting ${objectKey}`);
        }
      } catch (err) {
        console.error(`Error processing object ${objectKey}:`, err);
      }
    }

    const endTime = performance.now();
    const totalTime = (endTime - startTime) / 1000;

    console.log(
      `Experiment ${experimentNumber}, Batch ${batchNumber} - DocumentDB:`
    );
    console.log(`Process Total time: ${totalTime.toFixed(2)} seconds`);
    console.log(
      `Total data size: ${(totalDataSize / (1024 * 1024 * 1024)).toFixed(2)} GB`
    );
    console.log(`Request count: ${requestCount}`);
    console.log(`Total request latency: ${totalRequestLatency} ms`);

    // Export metrics to CSV on S3
    const csvData = `${experimentNumber},${batchNumber},${totalTime.toFixed(
      2
    )},${(totalDataSize / (1024 * 1024 * 1024)).toFixed(
      2
    )},${requestCount},${totalRequestLatency.toFixed(2)}\n`;

    // Get the existing CSV data from S3
    const getObjectParams = {
      Bucket: s3BucketName,
      Key: csvFileName,
    };

    let existingCsvData = "";

    try {
      const getObjectCommand = new GetObjectCommand(getObjectParams);
      const getObjectResponse = await s3Client.send(getObjectCommand);
      existingCsvData = await getObjectResponse.Body.transformToString();
    } catch (err) {
      if (err.name !== "NoSuchKey") {
        throw err;
      }
    }

    const updatedCsvData = existingCsvData + csvData;

    // Upload the updated CSV data to S3
    const uploadParams = {
      Bucket: s3BucketName,
      Key: csvFileName,
      Body: updatedCsvData,
    };

    const putObjectCommand = new PutObjectCommand(uploadParams);
    await s3Client.send(putObjectCommand);

    console.log(`Metrics exported to s3://${s3BucketName}/${csvFileName}`);

    await client.close();
  } catch (err) {
    console.error("Error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal Server Error" }),
    };
  }
};

async function readObjectKeysFromS3(s3Client, s3BucketName, objectKeysCsv) {
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
