const { findSentimentScore } = require("./helpers/handle-process.js");
const { contextVariablesTable } = require("./helpers/handleContext.js");

async function main({
  dbConfig,
  cosConfig,
  cloudantConfig,
  nluConfig,
  results,
}) {
  await findSentimentScore(
    results.primaryObject,
    nluConfig.language,
    nluConfig.version,
    nluConfig.apikey,
    nluConfig.serviceUrl
  );

  const contextVariables = contextVariablesTable(results.logs);
  results.quaternaryObject = contextVariables;

  return {
    dbConfig,
    cosConfig,
    cloudantConfig,
    results,
  };
}

module.exports = {
  main,
};
