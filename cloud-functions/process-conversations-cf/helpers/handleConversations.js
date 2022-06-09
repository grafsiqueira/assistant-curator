const { phoneTable } = require("./handlePhoneLogs");

async function handleConversations(logs, assistantConfig) {
  const { transferNode, feedbackNode, relevantTopics, finalNode } =
    assistantConfig;
  const pathObjs = [];
  const groups = groupBy(logs);

  let feedback;
  let transfered;
  let relevance;
  let channel;

  const groupsAndIntervals = Object.entries(groups).map(([key, value]) => {
    channel = null;
    transfered = null;
    relevance = null;
    feedback = null;

    let startTime = value[0].response_timestamp;
    let finisTime = value[value.length - 1].response_timestamp;
    const interval = findInterval(startTime, finisTime);

    for (let log of groups[key]) {
      if (!channel) channel = checkChannel(log) ? "Phone" : "Chat";
      feedback = findFeedBack(log, feedbackNode);
      if (!transfered)
        transfered = checkIfTransfered(log, transferNode) ? 1 : 0;
      if (!relevance) relevance = checkRelevance(log, relevantTopics) ? 1 : 0;

      pathObjs.push({
        conversationID: key,
        originNode: findNodesVisited(log)[0],
        destineNode: "",
      });
      if (pathObjs[pathObjs.length - 2])
        pathObjs[pathObjs.length - 2].destineNode = findNodesVisited(log)[0];
    }

    return {
      idUser:
        typeof value[0].request.context.metadata !== "undefined"
          ? value[0].request.context.metadata.user_id
          : "",
      conversationID: key,
      channel: channel,
      startTime: value[0].request_timestamp,
      interval: interval,
      feedback: feedback ? feedback : 0,
      transfered: transfered,
      relevance: relevance,
    };
  });

  const phoneObjects = phoneTable(groupsAndIntervals, groups, finalNode);

  return {
    conversationTable: checkIfUserIsNew(groupsAndIntervals),
    phoneTable: phoneObjects,
    path: pathObjs,
  };
}

function groupBy(array) {
  return array.reduce((objectsByKeyValue, obj) => {
    const value = obj.request.context.conversation_id;
    objectsByKeyValue[value] = (objectsByKeyValue[value] || []).concat(obj);
    return objectsByKeyValue;
  }, {});
}

function findInterval(start, finish) {
  let interval = Math.abs(new Date(finish) - new Date(start));
  let resultingDate = new Date(interval);

  let seconds = resultingDate / 1000;
  return Math.abs(seconds);
}

function findFeedBack(log, feedbackNode) {
  let found = false;
  for (let searchedTitle of feedbackNode) {
    if (
      !found &&
      log.response.output.nodes_visited_details &&
      log.response.output.nodes_visited_details.some(
        (node) =>
          node.title == searchedTitle || node.dialog_node == searchedTitle
      )
    ) {
      found = true;
      break;
    }
    if (found) {
      const contextArray = Object.entries(log.response.context);
      for (let keyValuePair of contextArray) {
        const feedback = handleFeedback(keyValuePair);
        if (feedback) {
          return feedback;
        }
      }
    }
  }
}

function handleFeedback(keyValuePair) {
  if (keyValuePair[0] === "feedback") {
    if (typeof keyValuePair[1] === "string") {
      return parseInt(keyValuePair[1].match(/\d+/g));
    } else if (typeof keyValuePair[1] === "number") {
      return parseInt(keyValuePair[1]);
    }
  }
}

function checkChannel(log) {
  if (log.request.context.vgwSessionID) return true;
}

function checkIfTransfered(log, transferNode) {
  for (let searchedTitle of transferNode) {
    if (
      log.response.output.nodes_visited_details &&
      log.response.output.nodes_visited_details.some(
        (node) =>
          node.title == searchedTitle || node.dialog_node == searchedTitle
      )
    ) {
      return true;
    }
  }
}

function checkRelevance(log, relevantTopics) {
  for (let obj of log.response.intents)
    if (relevantTopics.some((topic) => topic == obj.intent)) return true;
}

function checkIfUserIsNew(groups) {
  let knownUsers = [];

  for (let group of groups) {
    if (knownUsers.includes(group.idUser)) group["newUser"] = 0;
    else {
      group["newUser"] = 1;
      knownUsers.push(group.idUser);
    }
  }
  return groups;
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

module.exports = {
  handleConversations,
};
