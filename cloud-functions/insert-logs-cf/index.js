const { sendLogsToDatabases } = require("./helpers/handle-process");

async function main({ dbConfig, cosConfig, cloudantConfig, results }) {
  return sendLogsToDatabases(dbConfig, cosConfig, cloudantConfig, results);
}

module.exports = {
  main,
};
