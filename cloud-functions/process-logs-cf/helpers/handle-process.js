const { connect, endConnection } = require("../database/db2");
const { getLogs } = require("./get-logs");
const { handleLogs } = require("./process-helpers");

function processLogs(connStr, table, assistantConfig) {
  return new Promise(async (resolve, reject) => {
    try {
      const conn = await connect(connStr);
      const logs = await getLogs(conn, table, assistantConfig);
      const logsToProcess = limitArraySize(logs);
      const primaryObject = await handleLogs(logsToProcess);
      endConnection(conn);
      resolve({ primaryObject, logs: logsToProcess });
    } catch (error) {
      reject(`${error}`);
    }
  });
}

function limitArraySize(logs) {
  //this function will limit the logs array size to be processed in this execution
  //taking in account the conversationID of each log. This is necessary to prevent the
  //same conversation being processed in two different executions.

  if (logs.length <= 500) {
    return logs;
  } else {
    const conversationIDs = [];
    let auxArray = [];

    for (let log of logs) {
      //this loop takes all the conversation IDs. We use it below to filter the log array to check how
      //each conversation affects the maximum lenght
      if (!conversationIDs.includes(log.response.context.conversation_id)) {
        conversationIDs.push(log.response.context.conversation_id);
      }
    }

    //lets see how many conversations we can process until our length reaches the 500 limit
    for (let id of conversationIDs) {
      //this filters the logs to have only one conversation:
      const conversation = logs.filter((log) => {
        if (log.response.context.conversation_id === id) {
          return log;
        }
      });

      if (auxArray.length === 0 && conversation.length >= 500) {
        //this means that the first conversation is to big and must be processed alone
        return conversation;
      } else {
        if (auxArray.length + conversation.length <= 500) {
          //here we concat conversations in auxArray until it's size is the desired 500 length
          auxArray = auxArray.concat(conversation);
        } else {
          break;
        }
      }
    }
    return auxArray;
  }
}

module.exports = {
  processLogs,
};
