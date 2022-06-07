const { checkIfNew } = require("../database/db2");
const { handleConversations } = require("./handleConversations");

async function handleLogs(conn, secondaryTableName, logs, assistantConfig) {
  let searchSqlString = [];
  let { conversationTable, phoneTable, path } = await handleConversations(
    logs,
    assistantConfig
  );
  for (let group of conversationTable) {
    if (group.newUser == 1) {
      let string = createWhereString(group.idUser);
      searchSqlString.push(string);
    }
  }
  let db2Checked = await checkIfNew(
    conn,
    secondaryTableName,
    searchSqlString.join(" or ")
  );
  const confirmedNewUsers = confirmNewUsers(conversationTable, db2Checked);
  return {
    conversationTable: confirmedNewUsers,
    phoneTable: phoneTable,
    path: path,
  };
}

function confirmNewUsers(groupedLogs, db2Checked) {
  let secondaryObject = [];

  for (let group of groupedLogs) {
    if (db2Checked.some((user) => user.IDUSER === group.idUser))
      group["newUser"] = 0;

    group.startTime = adjustTimestamp(group.startTime);
    secondaryObject.push(group);
  }
  return secondaryObject;
}

function createWhereString(idUser) {
  return `(iduser = '${idUser}')`;
}

function adjustTimestamp(timestamp) {
  const Timestamp = timestamp.split("T");

  const date = Timestamp[0].split("-").reverse().join(".");
  const time = Timestamp[1].split(".")[[0]];

  return { date: date, time: time };
}

module.exports = {
  handleLogs,
};
