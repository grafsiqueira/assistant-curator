const { connect, endConnection } = require("../database/db2");
const { getLogs } = require("./get-logs");
const { handleLogs } = require("./process-helpers");

function processLogs(connStr, table, assistantConfig) {
  return new Promise(async (resolve, reject) => {
    try {
      const conn = await connect(connStr);
      const logs = await getLogs(conn, table, assistantConfig);
      const primaryObject = await handleLogs(logs);
      endConnection(conn);
      resolve({ primaryObject, logs });
    } catch (error) {
      reject(`${error}`);
    }
  });
}

module.exports = {
  processLogs,
};
