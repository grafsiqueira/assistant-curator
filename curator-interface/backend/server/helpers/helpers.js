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
      const conversations = arrangeConversations(rows);
      const { conversationsByDay, intentsByDay } = sortByDay(conversations);
      console.log(intentsByDay);
      endConnection(conn);
      resolve({
        conversations: conversations,
        conversationsByDay: conversationsByDay,
        intentsByDay: intentsByDay,
      });
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

function sortByDay(arrangedConversations) {
  const conversationsByDay = {};
  const intentsByDay = {};

  Object.entries(arrangedConversations).map(([key, value]) => {
    const day = extractDay(value);
    if (conversationsByDay[day] && !conversationsByDay[day].includes(key))
      conversationsByDay[day].push(key);
    else conversationsByDay[day] = [key];

    for (let obj of value) {
      if (intentsByDay[day] && !intentsByDay[day].includes(obj.FIRSTINTENT)) {
        intentsByDay[day] = intentsByDay[day].concat(obj.FIRSTINTENT);
      } else if (!intentsByDay[day]) {
        intentsByDay[day] = [obj.FIRSTINTENT];
      }
    }
  });
  return { conversationsByDay: conversationsByDay, intentsByDay: intentsByDay };
}

function extractDay(conversationRows) {
  const firstTimeStamp = conversationRows[0].CLIENTTIMESTAMP.split(" ");
  return firstTimeStamp[0];
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
