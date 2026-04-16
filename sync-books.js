const axios = require("axios");
const { MongoClient } = require("mongodb");
const fs = require("fs");

// =============================================
// SOZLAMALAR
// =============================================
const PHONE = "+998990300902";
const PASSWORD = "123456";
const API_BASE = "https://backend.book.uz/admin-api";
const USER_API = "https://backend.book.uz/user-api";

const MONGO_URI =
  "mongodb://Dostonzero:Book@ac-zdrgs6e-shard-00-00.smnoitk.mongodb.net:27017,ac-zdrgs6e-shard-00-01.smnoitk.mongodb.net:27017,ac-zdrgs6e-shard-00-02.smnoitk.mongodb.net:27017/?ssl=true&replicaSet=atlas-q2p47m-shard-0&authSource=admin&appName=Book-uz";
const DB_NAME = "bookuz";
const COLLECTION = "products";

const CACHE_FILE = "./books_cache.json";
const BATCH_SIZE = 200;
const PAGE_SIZE = 50;
const SLUG_PARALLEL = 10;
// =============================================

// =============================================
// 1. TOKEN OLISH
// =============================================
async function getToken() {
  console.log("🔐 Token olinmoqda...");
  const res = await axios.post(`${API_BASE}/sign-in`, {
    phoneNumber: PHONE,
    password: PASSWORD,
  });
  const token = res.data.data.token;
  console.log("✅ Token olindi\n");
  return token;
}

// =============================================
// 2. BARCHA KITOBLARNI YUKLASH
// =============================================
async function fetchAllBooks(token) {
  // Cache bor bo'lsa — publisher ham qo'shilganmi tekshir
  if (fs.existsSync(CACHE_FILE)) {
    const cached = JSON.parse(fs.readFileSync(CACHE_FILE, "utf8"));
    const withPublisher = cached.filter((b) => b._publisherFetched).length;
    console.log(
      `📂 Cache topildi: ${cached.length} ta kitob (${withPublisher} ta publisher bor)`,
    );
    console.log("   (Qayta yuklash uchun books_cache.json ni o'chiring)\n");
    return cached;
  }

  const client = axios.create({
    baseURL: API_BASE,
    headers: { Authorization: `Bearer ${token}` },
    timeout: 30000,
  });

  const allBooks = [];
  let page = 1;
  let total = null;

  console.log("📚 Kitoblar yuklanmoqda...\n");

  while (true) {
    try {
      const res = await client.get(`/book?page=${page}&limit=${PAGE_SIZE}`);
      const raw = res.data.data;
      const books = raw.data || [];
      total = raw.total || total;

      if (!Array.isArray(books) || books.length === 0) break;

      allBooks.push(...books);

      process.stdout.write(
        `\r⬇️  ${allBooks.length}/${total} ta yuklandi (sahifa ${page}/${Math.ceil(total / PAGE_SIZE)})`,
      );

      if (allBooks.length >= total || books.length < PAGE_SIZE) break;

      page++;
      await sleep(100);
    } catch (err) {
      console.error(
        `\n❌ Sahifa ${page}: ${err.response?.data?.message || err.message}`,
      );
      if (err.response?.status === 401) throw new Error("TOKEN_EXPIRED");
      await sleep(2000);
    }
  }

  console.log(`\n✅ Jami ${allBooks.length} ta kitob yuklandi\n`);
  fs.writeFileSync(CACHE_FILE, JSON.stringify(allBooks));
  console.log(`💾 Cache saqlandi: ${CACHE_FILE}\n`);

  return allBooks;
}

// =============================================
// 3. PUBLISHER OLISH VA books_cache GA QOSHISH
// =============================================
async function fetchAndMergePublishers(books) {
  // Publisher hali olinmagan kitoblar
  const toFetch = books.filter((b) => b.link && !b._publisherFetched);

  if (toFetch.length === 0) {
    console.log("✅ Barcha publisher ma'lumotlari mavjud\n");
    return books;
  }

  console.log(`🌐 ${toFetch.length} ta kitob uchun publisher yuklanmoqda...\n`);

  // books ni id bo'yicha map qilamiz (tez topish uchun)
  const bookMap = {};
  books.forEach((b, idx) => (bookMap[b._id] = idx));

  let done = 0;
  let failed = 0;

  for (let i = 0; i < toFetch.length; i += SLUG_PARALLEL) {
    const batch = toFetch.slice(i, i + SLUG_PARALLEL);

    const results = await Promise.allSettled(
      batch.map((book) =>
        axios.get(`${USER_API}/book/${book.link}`, { timeout: 15000 }),
      ),
    );

    results.forEach((result, idx) => {
      const book = batch[idx];
      const bookIdx = bookMap[book._id];

      if (result.status === "fulfilled") {
        const data = result.value.data?.data;
        if (data) {
          // Publisher ma'lumotini to'g'ridan-to'g'ri kitob ichiga qo'shamiz
          books[bookIdx].publisher = data.publisher || null;
          books[bookIdx]._publisherFetched = true;
          done++;
        }
      } else {
        // Publisher topilmasa ham belgilab qo'yamiz (qayta urinmaslik uchun)
        books[bookIdx]._publisherFetched = true;
        books[bookIdx].publisher = null;
        failed++;
      }
    });

    process.stdout.write(
      `\r📥 ${done + failed}/${toFetch.length} | ✅ ${done} | ❌ ${failed}`,
    );

    // Har 500 ta da cache saqlash (uzilsa davom etish uchun)
    if ((done + failed) % 500 === 0) {
      fs.writeFileSync(CACHE_FILE, JSON.stringify(books));
    }

    await sleep(100);
  }

  // Final cache saqlash
  fs.writeFileSync(CACHE_FILE, JSON.stringify(books));
  console.log(`\n✅ Publisher ma'lumotlari books_cache ga saqlandi\n`);

  return books;
}

// =============================================
// 4. FIELD MAPPING
// =============================================
function slugify(text) {
  return String(text || "")
    .toLowerCase()
    .trim()
    .replace(/['"`]/g, "")
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9\-]/g, "")
    .replace(/-+/g, "-");
}

function mapBook(book) {
  const baseSlug = book.link || slugify(book.name);
  const publisherInfo = book.publisher || null;

  return {
    externalId: book._id,

    title: {
      uz: book.name || "",
      ru: book.name || "",
      en: book.name || "",
    },

    slug: `${baseSlug}-${book._id}`,

    description: {
      uz: Array.isArray(book.description)
        ? book.description.map((d) => d?.value || "").join(" ")
        : book.description || "",
      ru: "",
      en: "",
    },

    price: book.bookPrice || 0,
    discountPrice: 0,

    images: [
      ...(book.imgUrl ? [book.imgUrl] : []),
      ...(Array.isArray(book.additionalImgs) ? book.additionalImgs : []),
    ],

    cover: book.cover || "",
    paperFormat: book.paperFormat || "",
    barcode: book.barcode || "",
    contentLanguage: book.contentLanguage || "",
    type: book.type || "single",
    label: book.label || "simple",
    state: book.state || "",

    stock: book.amount || 0,
    soldCount: book.soldBookCount || 0,
    viewsCount: book.viewsCount || 0,

    // Publisher — slug API dan olingan, books_cache ichida saqlangan
    publisher: publisherInfo
      ? {
          id: publisherInfo._id || null,
          name: publisherInfo.name || "",
          image: publisherInfo.imgUrl || "",
        }
      : null,

    author:
      book.authors?.map((a) => ({
        id: a._id || null,
        name: a.fullName || "",
      })) || "",

    category:
      book.genres?.map((gen) => ({
        id: gen._id || null,
        name: gen.name || "",
      })) || [],
    barcode: book.barcode || "",

    isAvailableAudio: book.isAvailableAudio || false,
    isAvailableEbook: book.isAvailableEbook || false,
    audioPrice: book.audioPrice || 0,
    ebookPrice: book.ebookPrice || 0,

    language: ["uz", "ru", "en"].includes(book.language) ? book.language : "uz",
    year: book.year || null,
    numberOfPage: book.numberOfPage || 0,

    isTop: false,
    isDiscount: false,

    ratingAvg: book.rating || 0,
    ratingCount: book.rateCount || 0,
    tags: book.tags || [],

    createdAt: book.createdAt ? new Date(book.createdAt) : new Date(),
    updatedAt: book.updatedAt ? new Date(book.updatedAt) : new Date(),
    syncedAt: new Date(),
  };
}

// =============================================
// 5. MONGODB GA YOZISH
// =============================================
async function saveToMongo(books) {
  const client = new MongoClient(MONGO_URI);

  try {
    await client.connect();
    console.log("🔗 MongoDB ga ulandi\n");

    const col = client.db(DB_NAME).collection(COLLECTION);
    await col.createIndex({ externalId: 1 }, { unique: true });

    let inserted = 0;
    let updated = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < books.length; i += BATCH_SIZE) {
      const batch = books.slice(i, i + BATCH_SIZE);

      const operations = batch.map((book) => ({
        updateOne: {
          filter: { externalId: book._id },
          update: { $set: mapBook(book) },
          upsert: true,
        },
      }));

      try {
        const result = await col.bulkWrite(operations, { ordered: false });
        inserted += result.upsertedCount;
        updated += result.modifiedCount;
      } catch (err) {
        const writeErrors =
          err.result?.result?.writeErrors?.length || batch.length;
        failed += writeErrors;
        inserted += err.result?.result?.nUpserted || 0;
        updated += err.result?.result?.nModified || 0;
      }

      const done = Math.min(i + BATCH_SIZE, books.length);
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = done / elapsed;
      const eta = ((books.length - done) / rate).toFixed(0);

      process.stdout.write(
        `\r💾 ${done}/${books.length} | ✅ Yangi: ${inserted} | 🔄 Updated: ${updated} | ❌ Xato: ${failed} | ~${eta}s qoldi`,
      );
    }

    const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log(`\n\n${"=".repeat(55)}`);
    console.log(`📊 NATIJA:`);
    console.log(`   ✅ Yangi qo'shildi : ${inserted} ta`);
    console.log(`   🔄 Yangilandi      : ${updated} ta`);
    console.log(`   ❌ Xato            : ${failed} ta`);
    console.log(`   ⏱️  Vaqt            : ${totalTime} soniya`);
    console.log(
      `   ⚡ Tezlik           : ${(books.length / totalTime).toFixed(0)} kitob/s`,
    );
    console.log(`${"=".repeat(55)}`);

    if (failed === 0) {
      if (fs.existsSync(CACHE_FILE)) fs.unlinkSync(CACHE_FILE);
      console.log("\n🗑️  Cache o'chirildi");
    }
  } finally {
    await client.close();
    console.log("🔌 MongoDB ulanishi yopildi");
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// =============================================
// MAIN
// =============================================
async function main() {
  console.log("📖 book.uz → MongoDB Sinxronizatsiya");
  console.log("=====================================\n");

  try {
    const token = await getToken();

    // 1. Barcha kitoblarni yuklash
    const books = await fetchAllBooks(token);
    if (books.length === 0) {
      console.log("⚠️  Kitob topilmadi.");
      return;
    }

    // 2. Publisher olish va books_cache ga qo'shish
    const booksWithPublisher = await fetchAndMergePublishers(books);

    // 3. MongoDB ga yozish
    await saveToMongo(booksWithPublisher);

    console.log("\n🎉 Sync muvaffaqiyatli tugadi!");
  } catch (err) {
    if (err.message === "TOKEN_EXPIRED") {
      console.error("🔐 Token yangilash kerak");
    } else {
      console.error("\n💥 Xato:", err.message);
    }
    process.exit(1);
  }
}

// async function testThreeBooks() {
//   console.log("🧪 3 ta kitob test qilinmoqda...\n");

//   const token = await getToken();

//   const client = axios.create({
//     baseURL: API_BASE,
//     headers: { Authorization: `Bearer ${token}` },
//     timeout: 30000,
//   });

//   // 3 ta kitob olamiz
//   const res = await client.get(`/book?page=1&limit=3`);
//   const books = res.data.data?.data || [];

//   if (books.length === 0) {
//     console.log("❌ Kitob topilmadi");
//     return;
//   }

//   for (let i = 0; i < books.length; i++) {
//     const book = books[i];

//     console.log(`\n==============================`);
//     console.log(`📘 ${i + 1}-kitob:`, book.name);
//     console.log("🔗 Slug/link:", book.link);

//     try {
//       const detailRes = await axios.get(`${USER_API}/book/${book.link}`, {
//         timeout: 15000,
//       });

//       const detail = detailRes.data?.data;
//       console.log("✅ user-api ishladi");
//       console.log("🏢 Publisher:", detail?.publisher || null);

//       book.publisher = detail?.publisher || null;
//       book._publisherFetched = true;
//     } catch (err) {
//       console.log("❌ user-api xato:", err.response?.data || err.message);
//       book.publisher = null;
//       book._publisherFetched = true;
//     }

//     console.log("TEST BOOK ID:", book._id);
//     console.log("TEST BOOK LINK:", book.link);
//     console.log("TEST BOOK PUBLISHER:", book.publisher);

//     const mapped = mapBook(book);
//     console.log("\n🧩 Mongo object:");
//     console.dir(mapped, { depth: null });
//   }

//   // hammasini birga saqlash
//   await saveToMongo(books);

//   console.log("\n🎉 3 ta kitob test tugadi");
// }

main();
