const axios = require("axios");
const { MongoClient } = require("mongodb");
 
const MONGO_URI = "mongodb://Dostonzero:Book@book-uz-shard-00-00.smnoitk.mongodb.net:27017,book-uz-shard-00-01.smnoitk.mongodb.net:27017,book-uz-shard-00-02.smnoitk.mongodb.net:27017/bookuz?ssl=true&authSource=admin&retryWrites=true&w=majority";
const DB_NAME = "bookuz";
const COLLECTION = "books";
 
async function checkSlug() {
  // 1. Bitta kitobning slug API sidan qanday kelishini ko'rish
  console.log("🔍 Slug API strukturasi:\n");
  try {
    const res = await axios.get("https://backend.book.uz/user-api/book/shohona-ishrat");
    console.log(JSON.stringify(res.data, null, 2));
  } catch (err) {
    console.log("❌", err.response?.status, err.response?.data || err.message);
  }
 
  // 2. MongoDB da bitta kitob qanday ko'rinishda ekanini ko'rish
  console.log("\n\n📦 MongoDB dagi bitta kitob:\n");
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const col = client.db(DB_NAME).collection(COLLECTION);
    const book = await col.findOne({});
    console.log(JSON.stringify(book, null, 2));
  } finally {
    await client.close();
  }
}
 
checkSlug().catch(console.error);
 