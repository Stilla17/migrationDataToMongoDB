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
const MOYSKLAD_ASSORTMENT_API =
  "https://api.moysklad.ru/api/remap/1.2/entity/assortment";
const MOYSKLAD_TOKEN = "b95833e6e3d48074a273e1ef4bcf3bceb23bb7e8";

const MONGO_URI =
  "mongodb://Dostonzero:Book@ac-zdrgs6e-shard-00-00.smnoitk.mongodb.net:27017,ac-zdrgs6e-shard-00-01.smnoitk.mongodb.net:27017,ac-zdrgs6e-shard-00-02.smnoitk.mongodb.net:27017/?ssl=true&replicaSet=atlas-q2p47m-shard-0&authSource=admin&appName=Book-uz";
const DB_NAME = "bookuz";
const COLLECTION = "products";

const CACHE_FILE = "./books_cache.json";
const BATCH_SIZE = 200;
const PAGE_SIZE = 50;
const SLUG_PARALLEL = 10;
const MOYSKLAD_PARALLEL = 5;
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
// 3. PUBLISHER OLISH
// =============================================
async function fetchAndMergePublishers(books) {
  const toFetch = books.filter((b) => b.link && !b._publisherFetched);

  if (toFetch.length === 0) {
    console.log("✅ Barcha publisher ma'lumotlari mavjud\n");
    return books;
  }

  console.log(`🌐 ${toFetch.length} ta kitob uchun publisher yuklanmoqda...\n`);

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
          books[bookIdx].publisher = data.publisher || null;
          books[bookIdx]._publisherFetched = true;
          done++;
        }
      } else {
        books[bookIdx]._publisherFetched = true;
        books[bookIdx].publisher = null;
        failed++;
      }
    });

    process.stdout.write(
      `\r📥 ${done + failed}/${toFetch.length} | ✅ ${done} | ❌ ${failed}`,
    );

    if ((done + failed) % 500 === 0) {
      fs.writeFileSync(CACHE_FILE, JSON.stringify(books));
    }

    await sleep(100);
  }

  fs.writeFileSync(CACHE_FILE, JSON.stringify(books));
  console.log(`\n✅ Publisher ma'lumotlari saqlandi\n`);

  return books;
}

// =============================================
// 3.5 MOYSKLAD NARXLARINI BARCODE ORQALI OLISH
// =============================================
async function fetchMoyskladPrices(books) {
  const booksWithBarcode = books.filter((b) => b.barcode);

  if (booksWithBarcode.length === 0) {
    console.log("⚠️  Barcode mavjud kitob yo'q, Moysklad skip qilindi\n");
    return books;
  }

  console.log(
    `🏷️  Moysklad: ${booksWithBarcode.length} ta kitob uchun narx tekshirilmoqda...\n`,
  );

  const bookMap = {};
  books.forEach((b, idx) => (bookMap[b._id] = idx));

  let found = 0;
  let notFound = 0;
  let failed = 0;
  const MOYSKLAD_PARALLEL = 5;

  for (let i = 0; i < booksWithBarcode.length; i += MOYSKLAD_PARALLEL) {
    const batch = booksWithBarcode.slice(i, i + MOYSKLAD_PARALLEL);

    const results = await Promise.allSettled(
      batch.map((book) =>
        axios.get(MOYSKLAD_ASSORTMENT_API, {
          headers: { Authorization: `Bearer ${MOYSKLAD_TOKEN}` },
          params: { filter: `barcode=${normalizeBarcode(book.barcode)}` },
          timeout: 15000,
        }),
      ),
    );

    results.forEach((result, idx) => {
      const book = batch[idx];
      const bookIdx = bookMap[book._id];

      if (result.status === "fulfilled") {
        const rows = result.value.data?.rows;
        if (Array.isArray(rows) && rows.length > 0) {
          const product = rows[0];
          // salePrices.value Moyskladda tiyin/kopeykada saqlnadi → /100
          const price = moyskladValueToPrice(product.salePrices?.[0]?.value);
          if (price != null) {
            books[bookIdx]._moyskladPrice = price;
            books[bookIdx]._moyskladFetched = true;
            found++;
          } else {
            books[bookIdx]._moyskladFetched = false;
            notFound++;
          }
        } else {
          books[bookIdx]._moyskladFetched = false;
          notFound++;
        }
      } else {
        books[bookIdx]._moyskladFetched = false;
        failed++;
      }
    });

    process.stdout.write(
      `\r🔍 ${i + batch.length}/${booksWithBarcode.length} | ✅ Topildi: ${found} | 🔶 Topilmadi: ${notFound} | ❌ Xato: ${failed}`,
    );

    await sleep(200); // Moysklad rate-limit
  }

  console.log(
    `\n✅ Moysklad narxlari yuklandi: ${found} ta kitobga narx o'rnatildi\n`,
  );
  return books;
}

// =============================================
// 4. MOYSKLAD NARXLARINI FAQAT BOOK.UZDAGI BARCODE'LAR BO'YICHA OLISH
// =============================================
async function fetchMoyskladPriceMapForBooks(books) {
  const uniqueBarcodes = [
    ...new Set(books.map((book) => normalizeBarcode(book.barcode)).filter(Boolean)),
  ];

  if (uniqueBarcodes.length === 0) {
    console.log("вљ пёЏ  Barcode mavjud kitob yo'q, MoySklad skip qilindi\n");
    return {};
  }

  console.log(
    `рџЏ·пёЏ  MoySklad: faqat bizdagi ${uniqueBarcodes.length} ta barcode bo'yicha narx tekshirilmoqda...\n`,
  );

  const priceMap = {};
  let found = 0;
  let notFound = 0;
  let failed = 0;

  for (let i = 0; i < uniqueBarcodes.length; i += MOYSKLAD_PARALLEL) {
    const batch = uniqueBarcodes.slice(i, i + MOYSKLAD_PARALLEL);

    const results = await Promise.allSettled(
      batch.map((barcode) =>
        axios.get(MOYSKLAD_ASSORTMENT_API, {
          headers: { Authorization: `Bearer ${MOYSKLAD_TOKEN}` },
          params: { filter: `barcode=${barcode}` },
          timeout: 15000,
        }),
      ),
    );

    results.forEach((result, idx) => {
      const barcode = batch[idx];

      if (result.status !== "fulfilled") {
        failed++;
        return;
      }

      const product = result.value.data?.rows?.[0];
      const price = moyskladValueToPrice(product?.salePrices?.[0]?.value);

      if (price == null) {
        notFound++;
        return;
      }

      priceMap[barcode] = price;
      found++;
    });

    process.stdout.write(
      `\rрџ”Ќ ${Math.min(i + MOYSKLAD_PARALLEL, uniqueBarcodes.length)}/${uniqueBarcodes.length} | вњ… Topildi: ${found} | рџ”¶ Topilmadi: ${notFound} | вќЊ Xato: ${failed}`,
    );

    await sleep(200);
  }

  console.log(
    `\nвњ… MoySklad narxlari: ${found} ta barcode topildi, ${notFound} ta yo'q\n`,
  );

  return priceMap;
}

/**
 * MoySkladdan barcha tovarlarni yuklaydi va
 * { barcode -> salePrice } ko'rinishidagi map qaytaradi.
 *
 * MoySklad narxlari "tiyin" da (100 ga bo'lish kerak → so'm).
 * Agar valyuta UZS bo'lmasa, narx 0 bo'lib qaytadi.
 */
/* async function fetchAllMoyskladPriceMapDisabled() {
  // Default holatda MoySkladdan fresh narx olinadi.
  // Cache faqat USE_MOYSKLAD_CACHE=1 bo'lsa ishlatiladi.
  // Cache mavjud bo'lsa shu sessiyada qayta yuklamaymiz
  if (USE_MOYSKLAD_CACHE && fs.existsSync(MOYSKLAD_CACHE_FILE)) {
    const cached = JSON.parse(fs.readFileSync(MOYSKLAD_CACHE_FILE, "utf8"));
    const normalizedCache = {};
    for (const [barcode, price] of Object.entries(cached)) {
      const normalizedBarcode = normalizeBarcode(barcode);
      if (normalizedBarcode) normalizedCache[normalizedBarcode] = price;
    }
    const count = Object.keys(normalizedCache).length;
    console.log(`📂 MoySklad cache topildi: ${count} ta tovar\n`);
    return normalizedCache;
  }

  console.log("🏪 MoySkladdan narxlar yuklanmoqda...\n");

  const headers = {
    Authorization: `Bearer ${MOYSKLAD_TOKEN}`,
    "Content-Type": "application/json",
  };

  const priceMap = {}; // { "barcode_value": price_in_sum }
  let offset = 0;
  let totalLoaded = 0;
  let grandTotal = null;

  while (true) {
    try {
      const res = await axios.get(MOYSKLAD_API, {
        headers,
        timeout: 20000,
        params: {
          limit: MOYSKLAD_PAGE_SIZE,
          offset,
        },
      });

      const data = res.data;
      const rows = data.rows || [];
      grandTotal = grandTotal ?? data.meta?.size ?? "?";

      if (rows.length === 0) break;

      for (const product of rows) {
        const barcodes = product.barcodes || [];
        if (barcodes.length === 0) continue;

        // Aynan salePrices[0].value olinadi.
        const firstSalePrice = product.salePrices?.[0];

        // MoySklad narxlari minimal birlikda (tiyin) — 100 ga bo'lamiz
        const priceInSum = moyskladValueToPrice(firstSalePrice?.value);
        if (priceInSum == null) continue;

        // Har bir barcode uchun narxni saqlaymiz
        for (const bcObj of barcodes) {
          // Barcode turli formatlarda keladi: { ean13: "..." } yoki { code128: "..." }
          const bcValue =
            bcObj.ean13 ||
            bcObj.ean8 ||
            bcObj.code128 ||
            bcObj.gtin ||
            bcObj.upc ||
            Object.values(bcObj)[0];

          const normalizedBarcode = normalizeBarcode(bcValue);
          if (normalizedBarcode) priceMap[normalizedBarcode] = priceInSum;
        }
      }

      totalLoaded += rows.length;
      offset += MOYSKLAD_PAGE_SIZE;

      process.stdout.write(
        `\r🏪 ${totalLoaded}/${grandTotal} ta tovar yuklandi`,
      );

      if (rows.length < MOYSKLAD_PAGE_SIZE) break;

      await sleep(200); // MoySklad rate-limit uchun
    } catch (err) {
      const msg = err.response?.data?.errors?.[0]?.error || err.message;
      console.error(`\n❌ MoySklad xato (offset=${offset}): ${msg}`);
      if (err.response?.status === 401) {
        throw new Error("MOYSKLAD_TOKEN_EXPIRED");
      }
      await sleep(3000);
    }
  }

  const matched = Object.keys(priceMap).length;
  console.log(
    `\n✅ MoySklad: ${totalLoaded} ta tovar yuklandi, ${matched} ta barcode topildi\n`,
  );

  fs.writeFileSync(MOYSKLAD_CACHE_FILE, JSON.stringify(priceMap));
  console.log(`💾 MoySklad cache saqlandi: ${MOYSKLAD_CACHE_FILE}\n`);

  return priceMap;
}
*/

// =============================================
// 5. FIELD MAPPING (moyskladPrice parametri qo'shildi)
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

function normalizeBarcode(value) {
  return String(value || "")
    .trim()
    .replace(/[^0-9A-Za-z]/g, "");
}

function cleanBarcode(value) {
  return String(value || "").trim();
}

function hasOwn(object, key) {
  return Object.prototype.hasOwnProperty.call(object, key);
}

function moyskladValueToPrice(value) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return null;
  return Math.round(numericValue / 100);
}

function mapBook(book, moyskladPriceMap = {}) {
  const baseSlug = book.link || slugify(book.name);
  const publisherInfo = book.publisher || null;

  // ── MoySklad narxi ──────────────────────────────────────────────────────
  const originalBarcode = cleanBarcode(book.barcode);
  const barcode = normalizeBarcode(book.barcode);
  const hasMoyskladPrice = Boolean(barcode) && hasOwn(moyskladPriceMap, barcode);
  const moyskladPrice = hasMoyskladPrice ? moyskladPriceMap[barcode] : null;

  // Agar MoySkladda narx bo'lsa — uni ishlatamiz, aks holda book.uz narxi
  const finalPrice = hasMoyskladPrice ? moyskladPrice : book.bookPrice || 0;
  // ────────────────────────────────────────────────────────────────────────

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

    // Narxlar
    price: finalPrice,
    discountPrice: 0,
    priceSource: hasMoyskladPrice ? "moysklad" : "book.uz",

    images: [
      ...(book.imgUrl ? [book.imgUrl] : []),
      ...(Array.isArray(book.additionalImgs) ? book.additionalImgs : []),
    ],

    cover: book.cover || "",
    paperFormat: book.paperFormat || "",
    barcode: originalBarcode || barcode,
    barcodeNormalized: barcode,
    contentLanguage: book.contentLanguage || "",
    type: book.type || "single",
    label: book.label || "simple",
    state: book.state || "",

    stock: book.amount || 0,
    soldCount: book.soldBookCount || 0,
    viewsCount: book.viewsCount || 0,

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
      })) || [],

    category:
      book.genres?.map((gen) => ({
        id: gen._id || null,
        name: gen.name || "",
      })) || [],

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
// 6. MONGODB GA YOZISH
// =============================================
async function saveToMongo(books, moyskladPriceMap) {
  const client = new MongoClient(MONGO_URI);

  try {
    await client.connect();
    console.log("🔗 MongoDB ga ulandi\n");

    const col = client.db(DB_NAME).collection(COLLECTION);
    await col.createIndex({ externalId: 1 }, { unique: true });

    let inserted = 0;
    let updated = 0;
    let failed = 0;
    let moyskladMatched = 0; // MoySkladdan narx topilgan kitoblar
    const startTime = Date.now();

    for (let i = 0; i < books.length; i += BATCH_SIZE) {
      const batch = books.slice(i, i + BATCH_SIZE);

      const operations = batch.map((book) => {
        const mapped = mapBook(book, moyskladPriceMap);
        if (mapped.priceSource === "moysklad") moyskladMatched++;
        return {
          updateOne: {
            filter: { externalId: book._id },
            update: { $set: mapped },
            upsert: true,
          },
        };
      });

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

    console.log(`\n\n${"=".repeat(60)}`);
    console.log(`📊 NATIJA:`);
    console.log(`   ✅ Yangi qo'shildi     : ${inserted} ta`);
    console.log(`   🔄 Yangilandi          : ${updated} ta`);
    console.log(`   ❌ Xato                : ${failed} ta`);
    console.log(`   🏪 MoySklad narx topildi: ${moyskladMatched} ta kitob`);
    console.log(`   ⏱️  Vaqt               : ${totalTime} soniya`);
    console.log(
      `   ⚡ Tezlik             : ${(books.length / totalTime).toFixed(0)} kitob/s`,
    );
    console.log(`${"=".repeat(60)}`);

    if (failed === 0) {
      if (fs.existsSync(CACHE_FILE)) fs.unlinkSync(CACHE_FILE);
      console.log("\n🗑️  Cachelar o'chirildi");
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
  console.log("📖 book.uz → MongoDB Sinxronizatsiya (MoySklad narxlar bilan)");
  console.log(
    "=================================================================\n",
  );

  try {
    const token = await getToken();

    // 1. Barcha kitoblarni yuklash
    const books = await fetchAllBooks(token);
    if (books.length === 0) {
      console.log("⚠️  Kitob topilmadi.");
      return;
    }

    // 2. Publisher olish
    const booksWithPublisher = await fetchAndMergePublishers(books);

    // 3. MoySkladdan barcode → narx map yuklash
    const moyskladPriceMap = await fetchMoyskladPriceMapForBooks(booksWithPublisher);

    // Qancha barcode mos kelishini oldindan ko'rsatamiz
    const booksWithBarcode = booksWithPublisher.filter(
      (b) => {
        const barcode = normalizeBarcode(b.barcode);
        return Boolean(barcode) && hasOwn(moyskladPriceMap, barcode);
      },
    );
    console.log(
      `📌 Barcode bo'yicha mos keldi: ${booksWithBarcode.length}/${booksWithPublisher.length} ta kitob\n`,
    );

    // 4. MongoDB ga yozish
    await saveToMongo(booksWithPublisher, moyskladPriceMap);

    console.log("\n🎉 Sync muvaffaqiyatli tugadi!");
  } catch (err) {
    if (err.message === "TOKEN_EXPIRED") {
      console.error("🔐 book.uz token yangilash kerak");
    } else if (err.message === "MOYSKLAD_TOKEN_EXPIRED") {
      console.error("🔐 MoySklad token yangilash kerak");
    } else {
      console.error("\n💥 Xato:", err.message);
    }
    process.exit(1);
  }
}

main();
