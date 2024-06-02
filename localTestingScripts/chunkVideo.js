const fs = require("fs");
const path = require("path");
const csvWriter = require("csv-writer").createObjectCsvWriter;

const folderPath = path.resolve("./testVideos");
const chunkSize = 12 * 1024 * 1024;
const chunkedVideosFolder = path.resolve("./testChunkedVideos");
const maxConcurrentStreams = 10;

if (!fs.existsSync(chunkedVideosFolder)) {
  fs.mkdirSync(chunkedVideosFolder);
}

// Initialize the CSV writer
const csvFilePath = path.resolve("./test_video_chunks.csv");
const csvWriterInstance = csvWriter({
  path: csvFilePath,
  header: [
    { id: "videoName", title: "Video Name" },
    { id: "chunkIndex", title: "Chunk Index" },
  ],
});

// Declare videoQueue outside of the processNextVideo function
let videoQueue = [];
const chunkRecords = [];
// Function to process a single video file
function processVideo(file) {
  const videoPath = path.join(folderPath, file);
  const readStream = fs.createReadStream(videoPath, {
    highWaterMark: chunkSize,
  });
  let chunkIndex = 1;

  readStream.on("data", (chunk) => {
    const chunkFilename = `${file}-${chunkIndex.toString().padStart(2, "0")}`;
    const chunkFilePath = path.join(chunkedVideosFolder, chunkFilename);
    fs.writeFileSync(chunkFilePath, chunk);
    chunkRecords.push({ videoName: file, chunkIndex: chunkIndex });
    console.log(`Chunk saved: ${chunkFilename}`);
    chunkIndex++;
  });

  readStream.on("end", () => {
    console.log(`Processed video: ${file}`);
    processNextVideo(); // Process the next video in the queue
  });
}

// Function to process the next video in the queue
function processNextVideo() {
  if (videoQueue.length === 0) {
    // Write all records to the CSV file
    csvWriterInstance
      .writeRecords(chunkRecords)
      .then(() => {
        console.log("All chunks processed.");
      })
      .catch((err) => {
        console.error("Error writing CSV:", err);
      });
    return;
  }

  const file = videoQueue.shift();
  processVideo(file);
}

// Read the list of video files and add them to the queue
fs.readdir(folderPath, (err, files) => {
  if (err) {
    console.error("Error reading folder:", err);
    return;
  }

  videoQueue = files.slice(); // Create a copy of the files array

  // Start processing the first video
  const file = videoQueue.shift();
  processVideo(file);
});

console.log(`Chunks saved in: ${chunkedVideosFolder}`);
console.log(
  `CSV file with video names and chunk indices created at: ${csvFilePath}`
);
