async function handleLogs(logs) {
  let primaryObject = [];
  for (let log of logs) {
    primaryObject.push(handleLog(log));
  }
  return Promise.all(primaryObject);
}

let i = 0;
function handleLog(log) {
  return new Promise((resolve, reject) => {
    try {
      const intents = findIntents(log);
      const confidence = intentsConfidence(log);
      const nodeTitle = findNodesVisited(log)[0];
      const result = {
        idUser:
          log.request &&
          log.request.context &&
          log.request.context.metadata &&
          log.request.context.metadata.user_id
            ? log.request.context.metadata.user_id
            : "",
        conversationID: log.request.context.conversation_id,
        logID: log.log_id,
        clientMessage: log.request.input.text
          ? log.request.input.text.replace("'", "")
          : "",
        clientTimestamp: adjustTimestamp(log.request_timestamp),
        assistantMessage: findText(log),
        assistantTimestamp: adjustTimestamp(log.response_timestamp),
        nodeTitle: nodeTitle,
        sentiment: 0,
        firstIntent: intents[0] ? intents[0] : "",
        firstIntentConfidence: confidence[0] ? parseFloat(confidence[0]) : 0,
        intents: intents.join(","),
        intentsConfidence: confidence.join(","),
        entities: findEntities(log),
        error: "",
      };
      i++;
      resolve(result);
    } catch (error) {
      reject(error);
    }
  });
}

function adjustTimestamp(timestamp) {
  const Timestamp = timestamp.split("T");

  const date = Timestamp[0].split("-").reverse().join(".");
  const time = Timestamp[1].split(".")[[0]];

  return { date: date, time: time };
}

function findText(log) {
  let texts = [];
  for (let text of log.response.output.text) {
    texts.push(text.replace("'", ""));
  }
  return texts.join(",");
}

function findNodesVisited(log) {
  let nodesVisited = [];
  if (!log.response.output.nodes_visited_details) {
    nodesVisited.push("assistant Suggestion");
  } else {
    for (let nodeDetail of log.response.output.nodes_visited_details) {
      if (nodeDetail.title) {
        nodesVisited.push(nodeDetail.title);
      } else {
        nodesVisited.push(nodeDetail.dialog_node);
      }
    }
  }
  return nodesVisited;
}

function findIntents(log) {
  let intents = [];
  for (let intent of log.response.intents) {
    intents.push(intent.intent);
  }
  return intents;
}

function intentsConfidence(log) {
  let confidence = [];
  for (let intent of log.response.intents) {
    confidence.push(intent.confidence);
  }
  return confidence;
}

function findEntities(log) {
  let entities = [];
  for (let entity of log.response.entities) {
    entities.push(entity.entity);
  }
  return entities.join(",");
}

module.exports = {
  handleLogs,
};
