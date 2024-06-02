const mongoose = require("mongoose");
const Schema = mongoose.Schema;

// Define the schema for video metadata
const videoSchema = new Schema({
  name: String,
  extension: String,
  chunkCount: Number,
  chunks: [{ type: Schema.Types.ObjectId, ref: "Chunk" }],
});

// Define the schema for individual chunks
const chunkSchema = new Schema({
  videoId: { type: Schema.Types.ObjectId, ref: "Video" },
  data: Buffer,
  order: Number,
});

// Create the models
const Video = mongoose.model("Video", videoSchema);
const Chunk = mongoose.model("Chunk", chunkSchema);

// Export the models
module.exports = {
  Video,
  Chunk,
};
