import { S3Client } from "@aws-sdk/client-s3";
import { GetObjectCommand } from "@aws-sdk/client-s3";

export const handler = async (event, context) => {
  try {
    // Create an S3 instance
    const s3 = new S3Client({ region: "us-east-2" });

    const s3BucketName = process.env.S3_BUCKET_NAME;
    const objectKeysCsv = "object-keys.csv";

    // Read object keys from the CSV file in the same S3 bucket
    const getObjectParams = {
      Bucket: s3BucketName,
      Key: objectKeysCsv,
    };

    let objectKeysCsvData = "";

    try {
      const getObjectCommand = new GetObjectCommand(getObjectParams);
      const getObjectResponse = await s3.send(getObjectCommand);
      objectKeysCsvData = await getObjectResponse.Body.transformToString();
    } catch (err) {
      console.error(`Error reading ${objectKeysCsv} from S3:`, err);
      throw err;
    }

    const objectKeys = objectKeysCsvData.split("\n");

    // Split the object keys into tempBatches of 1000
    const batches = [];
    const tempBatches = [];
    let batch = [];

    for (const key of objectKeys) {
      batch.push(key);

      if (batch.length === 1000) {
        tempBatches.push(batch);
        batch = [];
      }
    }

    // Add the last batch if it's not empty
    if (batch.length > 0) {
      tempBatches.push(batch);
    }

    for (let i = 1; i <= 10; i++) {
      batches.push({
        s3KeyBatch: tempBatches[0],

        experimentNumber: i,

        batchNumber: 1,
      });
    }

    for (let i = 1; i <= 10; i++) {
      batches.push({
        s3KeyBatch: tempBatches[1],

        experimentNumber: i,

        batchNumber: 2,
      });
    }

    console.log("Number of Batches: " + batches.length);

    return {
      statusCode: 200,
      body: JSON.stringify(batches),
    };
  } catch (err) {
    console.error("Error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal Server Error" }),
    };
  }
};
