const {
  select,
  update,
  connect,
  endConnection,
} = require("../common/database/db2");
const axios = require("axios");

function getConversations({ connStr, table }) {
  return new Promise(async (resolve, reject) => {
    try {
      const conn = await connect(connStr);
      const rows = await select(conn, table, "score is null");
      const result = arrangeConversations(rows);
      endConnection(conn);
      resolve(result);
    } catch {
      resolve(null);
    }
  });
}

function arrangeConversations(rows) {
  let arrangedConversations = {};
  for (let row of rows) {
    if (arrangedConversations[row.CONVERSATIONID])
      arrangedConversations[row.CONVERSATIONID].push(row);
    else arrangedConversations[row.CONVERSATIONID] = [row];
  }
  return arrangedConversations;
}

async function updateConversation(conversations, connStr, table) {
  return new Promise(async (resolve, reject) => {
    try {
      const conn = await connect(connStr);
      for (let conversation of Object.keys(conversations)) {
        for (let log of conversations[conversation]) {
          if (log.SCORE !== null)
            await update(
              conn,
              table,
              `score = ${log.SCORE}`,
              `logId = '${log.LOGID}'`
            );
        }
      }

      endConnection(conn);
      resolve("Conversation updated");
    } catch (error) {
      console.log(error);
      reject(error);
    }
  });
}

function registerLogin(docId, document) {
  return new Promise(async (resolve, reject) => {
    try {
      resolve(
        await axios.post(process.env.URL, { docId: docId, document: document })
      );
    } catch (err) {
      console.log("No URL received");
      resolve("Register Failure");
    }
  });
}

module.exports = {
  getConversations,
  updateConversation,
  arrangeConversations,
  registerLogin,
};
