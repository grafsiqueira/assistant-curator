const {
  insertOnCos,
  insertOnDb2,
  insertOnCloudant,
} = require("./process-helpers");

function sendLogsToDatabases(dbConfig, cosConfig, cloudantConfig, results) {
  return new Promise(async (resolve, reject) => {
    if (results.logs.length > 0) {
      try {
        await Promise.all([
          insertOnCos(cosConfig, results.logs),
          insertOnDb2(dbConfig, results),
          insertOnCloudant(cloudantConfig, results.logs),
        ]).then(() =>
          resolve({
            result: `${results.logs.length} logs were processed and ${results.primaryObject.length} were inserted!`,
          })
        );
      } catch (error) {
        reject(error);
      }
    } else {
      resolve({ result: "Nothing to insert." });
    }
  });
}

module.exports = {
  sendLogsToDatabases,
};
