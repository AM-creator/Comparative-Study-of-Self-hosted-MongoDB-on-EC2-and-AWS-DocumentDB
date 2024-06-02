const { Video, Chunk } = require("./utils/models");
const mongoose = require("./utils/db");
const path = require("path");
const fs = require("fs");

//mongoose.set("debug", true);
const startTime = new Date().getTime();
console.log("Start time:", startTime);
const chunkedVideosFolder = path.resolve("./testChunkedVideos");

// Group the chunked files by video name
const videoChunks = {};
fs.readdir(chunkedVideosFolder, (err, files) => {
  if (err) {
    console.error("Error reading folder:", err);
    return;
  }

  files.forEach((file) => {
    const videoName = file.split(".")[0];
    const filePath = path.join(chunkedVideosFolder, file);
    if (!videoChunks[videoName]) {
      videoChunks[videoName] = [];
    }
    videoChunks[videoName].push(filePath);
  });

  // Insert each chunked video into MongoDB
  insertChunkedVideos(videoChunks);
});

function insertChunkedVideos(videoChunks) {
  const videoPromises = Object.entries(videoChunks).map(
    ([videoName, chunkedFilePaths]) => {
      return insertChunkedVideo(videoName, ".mov", chunkedFilePaths);
    }
  );

  Promise.all(videoPromises)
    .then(() => {
      console.log("All videos saved successfully.");
      const endTime = new Date().getTime();
      console.log("End time:", endTime);
      const elapsedTime = endTime - startTime;
      console.log(`Elapsed time: ${elapsedTime} ms`);
      mongoose.disconnect();
    })
    .catch((err) => {
      console.error("Error saving videos:", err);
      mongoose.disconnect();
    });
}

function insertChunkedVideo(videoName, extension, chunkedFilePaths) {
  const newVideo = new Video({
    name: videoName,
    extension: extension,
    chunkCount: chunkedFilePaths.length,
    chunks: [],
  });

  return newVideo
    .save() // Return the promise from the save operation
    .then((savedVideo) => {
      const chunksPromises = chunkedFilePaths.map((filePath, index) => {
        const chunkData = fs.readFileSync(filePath);
        const newChunk = new Chunk({
          videoId: savedVideo._id,
          data: chunkData,
          order: index + 1,
        });
        return newChunk
          .save() // Return the promise from the save operation
          .then((savedChunk) => {
            savedVideo.chunks.push(savedChunk._id);
            console.log(
              `Video ${savedVideo.name}, Chunk ${index + 1} saved successfully`
            );
          })
          .catch((err) => {
            console.error(
              `Error saving video ${savedVideo.videoName} chunk ${index + 1}:`,
              err
            );
          });
      });

      return Promise.all(chunksPromises) // Wait for all chunk promises to resolve
        .then(() => {
          return savedVideo.save(); // Save the updated video with chunk references
        })
        .then(() => {
          console.log(
            `Video ${savedVideo.name} metadata and chunk references saved successfully`
          );
        })
        .catch((err) => {
          console.error(
            `Error saving video ${savedVideo.name} with chunk references:`,
            err
          );
        });
    })
    .catch((err) => {
      console.error(`Error saving video ${videoName} metadata:`, err);
    });
}
