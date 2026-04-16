const axios = require("axios");

async function main() {
  // 1. Avval token ol
  const loginRes = await axios.post("https://backend.book.uz/admin-api/sign-in", {
    phoneNumber: "+998990300902",
    password: "123456",
  });

  const token = loginRes.data.data.token; // <-- .data.data.token !
  console.log("✅ Token olindi:", token.slice(0, 30) + "...");

  // 2. Keyin kitoblarni yukla
  const res = await axios.get("https://backend.book.uz/admin-api/book?page=1&limit=5", {
    headers: {
      Authorization: `Bearer ${token}`,
    },
    timeout: 30000,
  });

  console.log("📦 Raw javob:");
  console.log(JSON.stringify(res.data, null, 2));
}

main().catch(err => {
  console.log("❌ Status:", err.response?.status);
  console.log("❌ Javob:", JSON.stringify(err.response?.data, null, 2));
  console.log("❌ Xato:", err.message);
});

const MOYSKLAD_API = "https://online.moysklad.ru/api/remap/1.2/entity/product";
const token = "b95833e6e3d48074a273e1ef4bcf3bceb23bb7e8";

async function getPriceMoysklad(price, book) {
  try {
    const res = await axios.get(`${MOYSKLAD_API}?filter=name=${book.name}`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
      timeout: 15000,
    });
  } catch (err) {
    console.log("Moysklad narx olishda xato:", err.message);
    return 0;
  }
}
