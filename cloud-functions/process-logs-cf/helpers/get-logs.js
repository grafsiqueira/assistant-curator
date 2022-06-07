const { lastRecord } = require("../database/db2");
const { queryAssistant } = require("./assistant");

async function getLogs(conn, table, assistantConfig) {
  const filter = (await lastRecord(conn, table))["1"];
  return queryAssistant(
    assistantConfig,
    null,
    filter ? filter.replace(" ", "T").substring(0, 20) + "999Z" : null,
    []
  );
}

module.exports = {
  getLogs,
};
