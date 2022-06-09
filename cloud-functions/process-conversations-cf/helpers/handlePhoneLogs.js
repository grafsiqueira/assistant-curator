function phoneTable(groupsAndIntervals, groups, finalNode) {
  const phoneObjects = [];

  groupsAndIntervals.map((group) => {
    if (group.channel == "Phone") {
      groups[group.conversationID].map((log) => {
        const checkMarks = checkLog(log);

        const auxObj = {
          idUser: group.idUser,
          conversationID: group.conversationID,
          userNumber:
            typeof log.request.context.integrations !== "undefined"
              ? log.request.context.integrations.voice_telephony.private
                  .user_phone_number
              : "",
          userIPAddress:
            typeof log.request.context.integrations !== "undefined"
              ? log.request.context.integrations.voice_telephony.private
                  .ip_address
              : "",
          vgwIsDTMF: checkMarks.vgwIsDTMF ? 1 : 0,
          vgwBargeInOccurred: checkMarks.vgwBargeInOccurred ? 1 : 0,
          vgwPhoneUserPhoneNumber: log.request.context.vgwPhoneUserPhoneNumber,
          vgwDTMFCollectionSucceeded: checkMarks.vgwDTMFCollectionSucceeded
            ? 1
            : 0,
          concluded: checkConclusion(log, finalNode) ? 1 : 0,
        };

        if (
          !phoneObjects.some(
            (obj) => obj.conversationID == group.conversationID
          )
        ) {
          phoneObjects.push(auxObj);
        } else {
          phoneObjects.map((obj) => {
            if (obj.conversationID == group.conversationID) {
              Object.entries(obj).map(([key, value]) => {
                if (key === undefined || key === 0) {
                  value = auxObj[key];
                }
              });
            }
          });
        }
      });
    }
  });
  return phoneObjects;
}

function checkLog(log) {
  const checkMarks = {
    vgwIsDTMF: false,
    vgwBargeInOccurred: false,
    vgwDTMFCollectionSucceeded: false,
  };

  if (log.request.context.vgwIsDTMF == "Yes") checkMarks.vgwIsDTMF = true;
  if (log.request.context.vgwBargeInOccurred == "Yes")
    checkMarks.vgwBargeInOccurred = true;
  if (log.request.context.vgwDTMFCollectionSucceeded == "Yes")
    checkMarks.vgwDTMFCollectionSucceeded = true;

  return checkMarks;
}

function checkConclusion(log, finalNode) {
  let found = false;
  for (let searchedTitle of finalNode) {
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
  }
  return found;
}

module.exports = {
  phoneTable,
};
