const { CloudantV1 } = require("@ibm-cloud/cloudant");
const { IamAuthenticator } = require("ibm-cloud-sdk-core");

function createCloudantClient(apiKey, url) {
  const authenticator = new IamAuthenticator({
    apikey: apiKey,
  });
  const client = CloudantV1.newInstance({
    authenticator: authenticator,
  });
  client.setServiceUrl(url);

  return client;
}

async function createDbAndDoc(client, dbName, docId, document) {
  return new Promise(async (resolve, reject) => {
    document._id = docId;
    try {
      const putDatabaseResult = (
        await client.putDatabase({
          db: dbName,
        })
      ).result;
      if (putDatabaseResult.ok) {
        console.log(`"${dbName}" database created.`);
      }
    } catch (err) {
      if (err.code === 412) {
        console.log(
          `Cannot create "${dbName}" database, it already exists. Will connect to existing Db...`
        );
      } else {
        reject(err);
      }
    }

    try {
      const createDocumentResponse = await client.postDocument({
        db: dbName,
        document: document,
      });

      // Keep track with the revision number of the document object
      document._rev = createDocumentResponse.result.rev;
      resolve("Document created with success.");
    } catch (err) {
      if (err.code === 409) {
        console.log(
          `Cannot create document, as it already exists. Will try updating it instead...`
        );
        resolve(await updateDoc(client, dbName, docId, document));
      } else {
        reject(err);
      }
    }
  });
}

async function updateDoc(client, dbName, docId, document) {
  return new Promise(async (resolve, reject) => {
    // Try to get the document if it previously existed in the database
    try {
      const existingDocument = (
        await client.getDocument({
          docId: docId,
          db: dbName,
        })
      ).result;

      document._rev = existingDocument._rev;
      document._id = docId;

      await client.postDocument({
        db: dbName,
        document: document,
      });

      resolve("Document updated with success.");
    } catch (err) {
      if (err.code === 404) {
        reject(
          `Cannot update document because either "${dbName}" database or document was not found.`
        );
      }
    }
  });
}

async function getDoc(client, dbName, docId) {
  return new Promise(async (resolve, reject) => {
    try {
      const getDocParams = { db: dbName, docId: docId };
      const response = await client.getDocument(getDocParams);
      const { result } = response;

      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

module.exports = {
  createCloudantClient,
  createDbAndDoc,
  getDoc,
};
