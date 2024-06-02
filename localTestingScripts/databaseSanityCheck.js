const { Video, Chunk } = require("./utils/models");
const mongoose = require("./utils/db");
const path = require("path");
const fs = require("fs");
mongoose.set("debug", true);

async function retrieveVideos() {
  const testVideo = "0000f77c-6257be58";
  const folderPath = path.resolve("./retrivedVideos");

  try {
    const video = await Video.findOne({ name: testVideo }).populate("chunks");
    const videoName = video.name;
    console.log(`Retrieving video: ${videoName}`);
    const extension = video.extension;

    // Sort the chunks based on their order
    const sortedChunks = video.chunks.sort((a, b) => a.order - b.order);

    // Concatenate the chunk data in the correct order
    const videoData = Buffer.concat(sortedChunks.map((chunk) => chunk.data));

    console.log(`Video: ${videoName}${extension} retrieved`);

    const filePath = path.join(folderPath, `${videoName}${extension}`);

    if (!fs.existsSync(filePath)) {
      fs.mkdirSync(filePath);
    }
    fs.writeFileSync(filePath, videoData);
    console.log(`File created: ${filePath}`);

    mongoose.disconnect();
    process.exit();
  } catch (error) {
    console.error("Error retrieving videos:", error);
    process.exit(1);
  }
}

retrieveVideos();
