const net = require("net");

const sendRequest = (request) => {
  return new Promise((resolve, reject) => {
    const client = new net.Socket();
    let responseData = "";

    client.connect(8080, "127.0.0.1", () => {
      client.write(JSON.stringify(request));
    });

    client.on("data", (data) => {
      responseData += data.toString(); // Accumulate response chunks
    });

    client.on("end", () => {
      // Parse the full response after socket ends
      try {
        const jsonResponse = JSON.parse(responseData);
        resolve(jsonResponse);
      } catch (err) {
        reject(new Error(`Failed to parse S62 response: ${err.message}`));
      }
    });

    client.on("error", (err) => {
      reject(new Error(`S62 Connection Error: ${err.message}`));
    });
  });
};

(async () => {
  try {
    const queryResponse = await sendRequest({
      query: "MATCH (c1:CUSTOMER)-[:PURCHASE_SAME_ITEM]->(c2:CUSTOMER) \
      WHERE c1.C_CUSTKEY <> c2.C_CUSTKEY AND c1.C_CUSTKEY + c2.C_CUSTKEY < 10000 \
      RETURN \
      c1.C_ADDRESS, c1.C_NATIONKEY, c1.C_ACCTBAL, c1.C_MKTSEGMENT, c1.C_COMMENT, c1.C_CUSTKEY AS src, \
      c2.C_ADDRESS, c2.C_NATIONKEY, c2.C_ACCTBAL, c2.C_MKTSEGMENT, c2.C_COMMENT, c2.C_CUSTKEY AS dst \
      LIMIT 400"
    });

    console.log("Query Response:", queryResponse);
  } catch (error) {
    console.error("Error:", error);
  }
})();
