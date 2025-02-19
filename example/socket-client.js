const net = require("net");

const sendRequest = (request) => {
  return new Promise((resolve, reject) => {
    const client = new net.Socket();
    client.connect(8080, "127.0.0.1", () => {
      client.write(JSON.stringify(request));
    });

    client.on("data", (data) => {
      resolve(JSON.parse(data.toString()));
      client.destroy();
    });

    client.on("error", reject);
  });
};

(async () => {
  try {
    // 1️⃣ Query 요청
    const queryResponse = await sendRequest({
      query: "MATCH (n:Person) RETURN n.id LIMIT 10"
    });
    console.log("Query Response:", queryResponse);

    // 2️⃣ Load 요청
    const loadResponse = await sendRequest({
      load: "/source-data/ldbc/sf1/static/Tag_hasType_TagClass.csv",
      label: "HAS_TYPE"
    });
    console.log("Load Response:", loadResponse);
    
  } catch (error) {
    console.error("Error:", error);
  }
})();
