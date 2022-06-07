const { connect, endConnection } = require("../database/db2");
const { handleLogs } = require("./process-helpers");

function processConversations(dbConfig, assistantConfig, results) {
  return new Promise(async (resolve, reject) => {
    try {
      const conn = await connect(dbConfig.connStr);
      const { logs } = results;
      const groupTableAndPhoneTableObjects = await handleLogs(
        conn,
        dbConfig.secondaryTableName,
        logs,
        assistantConfig
      );
      endConnection(conn);
      resolve(groupTableAndPhoneTableObjects);
    } catch (error) {
      reject(error);
    }
  });
}

module.exports = {
  processConversations,
};
