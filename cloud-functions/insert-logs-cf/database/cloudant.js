const Cloudant = require("@cloudant/cloudant");

function createCloudantClient(url, apiKey) {
  return Cloudant({
    url: url,
    plugins: { iamauth: { iamApiKey: apiKey } },
  });
}

async function createDb(client, dbName) {
  try {
    await client.db.create(dbName);
  } catch (err) {
    console.log("Db already exists! No need to create it.");
  }
}

function createDoc(client, dbName, insertion) {
  const db = client.db.use(dbName);
  db.insert(insertion, (err, response) => {
    if (err) {
      throw err;
    } else {
      return response;
    }
  });
}

function createDbAndDoc(client, dbName, insertion) {
  return new Promise(async (resolve, reject) => {
    try {
      await createDb(client, dbName);
      resolve(createDoc(client, dbName, insertion));
    } catch (err) {
      reject(err);
    }
  });
}

module.exports = {
  createCloudantClient,
  createDbAndDoc,
};
