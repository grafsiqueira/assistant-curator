const {
  createCloudantClient,
  createDbAndDoc,
  getDoc,
} = require("../common/database/cloudant");

async function insertOnCloudant(apikey, url, dbName, docId, document) {
  try {
    const client = createCloudantClient(apikey, url);
    return await createDbAndDoc(client, dbName, docId, document);
  } catch (err) {
    return { Error: "Unnable to connect with suplied credentials" };
  }
}

async function getFromCloudant(apiKey, url, dbName, docId) {
  try {
    const client = createCloudantClient(apiKey, url);
    return await getDoc(client, dbName, docId);
  } catch (err) {
    return { Error: "Unnable to connect with suplied credentials" };
  }
}

module.exports = {
  insertOnCloudant,
  getFromCloudant,
};
