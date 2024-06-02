import { MongoClient } from "mongodb";

//Create a MongoDB client, open a connection to DocDB; as a replica set,
//  and specify the read preference as secondary preferred
export const handler = async (event, context) => {
  try {
    const client = await MongoClient.connect(
      process.env.DOCUMENTDB_ENDPOINT,
      {
        tlsCAFile: `global-bundle.pem`, //Specify the DocDB; cert
      }
    );

    //Specify the database to be used
    const db = client.db("sample-database");

    //Specify the collection to be used
    const col = db.collection("sample-collection");

    //Insert a single document
    await col.insertOne({ hello: "Amazon DocumentDB" });

    //Find the document that was previously written
    const result = await col.findOne({ hello: "Amazon DocumentDB" });

    //Print the result to the screen
    console.log(result);

    //Close the connection
    await client.close();
  } catch (err) {
    console.error(err);
    return err;
  }
};
