const { processConversations } = require("./helpers/handle-process");
async function main({
  dbConfig,
  cosConfig,
  cloudantConfig,
  assistantConfig,
  nluConfig,
  results,
}) {
  const groupTableAndPhoneTableObjects = await processConversations(
    dbConfig,
    assistantConfig,
    results
  );

  results.secondaryObject = groupTableAndPhoneTableObjects.conversationTable;
  results.tertiaryObject = groupTableAndPhoneTableObjects.phoneTable;
  results.path = groupTableAndPhoneTableObjects.path;

  return {
    dbConfig,
    cosConfig,
    cloudantConfig,
    nluConfig,
    results,
  };
}

module.exports = {
  main,
};
