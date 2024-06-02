const { Video, Chunk } = require("./utils/models");
const mongoose = require("./utils/db");
mongoose.set("debug", true);

// Delete all documents in the 'Chunk' collection
Chunk.deleteMany({})
  .then(() => console.log("All Chunk documents deleted successfully"))
  .catch((err) => console.error("Error deleting Chunk documents:", err));

// Delete all documents in the 'Video' collection
Video.deleteMany({})
  .then(() => {
    console.log("All Video documents deleted successfully");
    process.exit();
  })
  .catch((err) => console.error("Error deleting Video documents:", err));
