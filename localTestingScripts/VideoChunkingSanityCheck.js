const fs = require("fs");
const path = require("path");

const videoPath = path.resolve("./testVideos/0000f77c-62c2a288.mov");
const chunkSize = 12 * 1024 * 1024;
const chunks = [];

const readStream = fs.createReadStream(videoPath, { highWaterMark: chunkSize });

readStream.on("data", (chunk) => {
  chunks.push(chunk);
  console.log(chunk);
});

readStream.on("end", () => {
  const combinedBuffer = Buffer.concat(chunks);
  console.log("Combined video data length:", combinedBuffer.length);

  const combinedFilePath = path.resolve("./combinedVideo.mov");
  fs.writeFile(combinedFilePath, combinedBuffer, (err) => {
    if (err) {
      console.error("Error writing combined video:", err);
    } else {
      console.log("Combined video file created:", combinedFilePath);
    }
  });
});
