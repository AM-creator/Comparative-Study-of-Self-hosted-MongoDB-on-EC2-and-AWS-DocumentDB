import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { Readable } from "stream";
import { MongoClient } from "mongodb";
import { performance } from "perf_hooks";

export const handler = async (event, context) => {
  try
  { const startTime = performance.now();    
    // Create an S3 instance
    const s3Client = new S3Client({ region: "us-east-2" });
    const s3BucketName = process.env.S3_BUCKET_NAME;
    const csvFileName = `mongodb-metrics.csv`;

    // Connect to MongoDB
    const client = await MongoClient.connect(process.env.MONGODB_URI);
    const db = client.db();
    console.log("Connected to MongoDB");

    // Create a collection for the video data
    const videoChunks = db.collection("VideoChunk");

    // Get the S3 key batch, experiment number, and batch number from the event input
    const s3KeyBatch = event.s3KeyBatch || [];
    const experimentNumber = event.experimentNumber || 0;
    const batchNumber = event.batchNumber || 0;

    // Initialize metrics
   
    let totalDataSize = 0;
    let requestCount = 0;
    let totalRequestLatency = 0;

    // Process the S3 keys and insert data into MongoDB
    for (const objectKey of s3KeyBatch) {
      try {
        // Fetch the object data from S3
        const getObjectCommand = new GetObjectCommand({
          Bucket: s3BucketName,
          Key: objectKey,
        });
        const objectData = await s3Client.send(getObjectCommand);

        // Convert the ReadableStream to a Buffer
        const chunks = [];
        const readableStream = objectData.Body;
        if (readableStream instanceof Readable) {
          for await (const chunk of readableStream) {
            chunks.push(chunk);
          }
        }
        const buffer = Buffer.concat(chunks);
        totalDataSize += buffer.length;

        // Insert the data into MongoDB
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

   console.log(`Experiment ${experimentNumber}-${batchNumber} - MongoDB:`);
    console.log(`Process Total time: ${totalTime.toFixed(2)} seconds`);
    console.log(`Total data size: ${(totalDataSize / (1024 * 1024 * 1024)).toFixed(2)} GB`); 
    console.log(`Request count: ${requestCount}`);
    console.log(`Total request latency: ${ totalRequestLatency} ms`);

    // Export metrics to CSV on S3
    const csvData = `${experimentNumber},${batchNumber},${totalTime.toFixed(
      2
    )},${(totalDataSize / (1024 * 1024 * 1024)).toFixed(2)},${requestCount},${totalRequestLatency.toFixed(
      2
    )}\n`;

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

    // Close the MongoDB connection
    await client.close();

     return {
      statusCode: 200,
      body: JSON.stringify({
        totalTime: totalTime.toFixed(2),
        totalDataSize: (totalDataSize / (1024 * 1024 * 1024)).toFixed(2),
        requestCount,
        totalRequestLatency: totalRequestLatency.toFixed(2),             
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
