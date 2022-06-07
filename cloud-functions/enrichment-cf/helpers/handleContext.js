function contextVariablesTable(logs) {
  let resultantArray = [];
  let auxArray = [
    "metadata",
    "system",
    "conversation_id",
    "integrations",
    "vgwAction",
    "vgwSIPFromURI",
    "vgwSTTResponse",
    "vgwTextAlternatives",
    "vgwIsDTMF",
    "vgwSIPCallID",
    "vgwTranscriptionSource",
    "vgwSIPRequestURI",
    "vgwBargeInOccurred",
    "vgwPhoneUserPhoneNumber",
    "vgwTenantID",
    "vgwSIPToURI",
    "vgwCompletedActions",
    "vgwIsCaller",
    "vgwSessionID",
    "vgwPostResponseTimeoutOccurred",
    "vgwDTMFCollectionSucceeded",
  ];

  for (let log of logs) {
    Object.entries(log.response.context).map(([key, value]) => {
      if (!auxArray.includes(key)) {
        if (
          !resultantArray.some(
            (obj) =>
              obj.conversationID == log.request.context.conversation_id &&
              obj.envVariableName == key &&
              obj.envVariableValue == (value || JSON.stringify(value)) &&
              obj.envVariableType == typeof value
          )
        ) {
          resultantArray.push({
            conversationID: log.request.context.conversation_id,
            envVariableName: key,
            envVariableValue: JSON.stringify(value).replace(/'/g, '"'),
            envVariableType: typeof value,
          });
        }
      }
    });
  }
  return resultantArray;
}

module.exports = {
  contextVariablesTable,
};
