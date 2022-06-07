const { processLogs } = require("./helpers/handle-process");

async function main({
  dbConfig,
  cosConfig,
  cloudantConfig,
  assistantConfig,
  nluConfig,
}) {
  const { connStr, primaryTableName } = dbConfig;
  const results = await processLogs(connStr, primaryTableName, assistantConfig);

  return {
    dbConfig,
    cosConfig,
    cloudantConfig,
    nluConfig,
    assistantConfig,
    results,
  };
}

module.exports = {
  main,
};
