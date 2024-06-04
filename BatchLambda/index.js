import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
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
    // Split the object keys into batches of 1000
    const batches = [];
    let batch = [];
    for (const key of objectKeys) {
      batch.push(key);
      if (batch.length === 1000) {
        batches.push(batch);
        batch = [];
      }
    }
    // Add the last batch if it's not empty
    if (batch.length > 0) {
      batches.push(batch);
    }

    console.log("Number of batches: " + batches.length);

    // Store the first batch in a CSV file named "batch1.csv"
    const batch1CsvData = batches[0].join("\n");
    const uploadParams1 = {
      Bucket: s3BucketName,
      Key: "batch1.csv",
      Body: batch1CsvData,
    };
    const putObjectCommand1 = new PutObjectCommand(uploadParams1);
    await s3.send(putObjectCommand1);

    // Store the second batch in a CSV file named "batch2.csv"
    const batch2CsvData = batches[1] ? batches[1].join("\n") : "";
    const uploadParams2 = {
      Bucket: s3BucketName,
      Key: "batch2.csv",
      Body: batch2CsvData,
    };
    const putObjectCommand2 = new PutObjectCommand(uploadParams2);
    await s3.send(putObjectCommand2);

    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Batches stored successfully" }),
    };
  } catch (err) {
    console.error("Error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal Server Error" }),
    };
  }
};
