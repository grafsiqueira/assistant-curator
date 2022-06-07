function phoneTable(groupsAndIntervals, groups, finalNode) {
  const phoneObjects = [];

  groupsAndIntervals.map((group) => {
    if (group.channel == "Phone") {
      groups[group.conversationID].map((log) => {
        const checkMarks = checkLog(log);
        if (
          !phoneObjects.some(
            (obj) => obj.conversationID == group.conversationID
          )
        ) {
          phoneObjects.push({
            idUser: group.idUser,
            conversationID: group.conversationID,
            userNumber:
              log.request.context.integrations.voice_telephony.private
                .user_phone_number,
            userIPAddress:
              log.request.context.integrations.voice_telephony.private
                .ip_address,
            vgwIsDTMF: checkMarks.vgwIsDTMF ? 1 : 0,
            vgwBargeInOccurred: checkMarks.vgwBargeInOccurred ? 1 : 0,
            vgwPhoneUserPhoneNumber:
              log.request.context.vgwPhoneUserPhoneNumber,
            vgwDTMFCollectionSucceeded: checkMarks.vgwDTMFCollectionSucceeded
              ? 1
              : 0,
            concluded: checkConclusion(log, finalNode) ? 1 : 0,
          });
        } else {
          phoneObjects.map((obj) => {
            if (obj.conversationID == group.conversationID) {
              if (obj.userNumber === undefined) {
                obj.userNumber =
                  log.request.context.integrations.voice_telephony.private.user_phone_number;
              }
              if (obj.userIPAddress === undefined) {
                obj.userIPAddress =
                  log.request.context.integrations.voice_telephony.private.ip_address;
              }
              if (obj.vgwIsDTMF === 0) {
                obj.vgwIsDTMF = checkMarks.vgwIsDTMF ? 1 : 0;
              }
              if (obj.vgwBargeInOccurred === 0) {
                obj.vgwBargeInOccurred = checkMarks.vgwBargeInOccurred ? 1 : 0;
              }
              if (obj.vgwPhoneUserPhoneNumber === undefined) {
                obj.vgwPhoneUserPhoneNumber =
                  log.request.context.vgwPhoneUserPhoneNumber;
              }
              if (obj.vgwDTMFCollectionSucceeded === 0) {
                obj.vgwPhoneUserPhoneNumber =
                  checkMarks.vgwDTMFCollectionSucceeded ? 1 : 0;
              }
              if (obj.concluded === 0) {
                obj.concluded = checkConclusion(log, finalNode) ? 1 : 0;
              }
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
  for (let searchedTitle of finalNode) {
    if (
      log.response.output.nodes_visited_details &&
      log.response.output.nodes_visited_details.some(
        (node) =>
          node.title == searchedTitle || node.dialog_node == searchedTitle
      )
    ) {
      return true;
    } else {
      return false;
    }
  }
}

module.exports = {
  phoneTable,
};
