const AssistantV1 = require("ibm-watson/assistant/v1");
const { IamAuthenticator } = require("ibm-watson/auth");

function queryAssistant(assistantConfig, cursor, lastRecord, fullResponse) {
  const assistant = new AssistantV1({
    version: assistantConfig.version,
    authenticator: new IamAuthenticator({
      apikey: assistantConfig.apiKey,
    }),
    serviceUrl: assistantConfig.serviceUrl,
  });

  return new Promise(async (resolve, reject) => {
    try {
      let filter = lastRecord ? `response_timestamp>=${lastRecord}` : "";
      const params = {
        workspaceId: assistantConfig.skillID,
        filter,
        sort: "request_timestamp",
        cursor,
        pageLimit: 100,
      };
      const response = await assistant.listLogs(params);

      fullResponse = fullResponse.concat(response.result.logs);

      if (response.result.pagination.next_cursor) {
        return resolve(
          await queryAssistant(
            assistantConfig,
            response.result.pagination.next_cursor,
            null,
            fullResponse
          )
        );
      } else {
        return resolve(fullResponse);
      }
    } catch (error) {
      reject(error);
    }
  });
}

module.exports = { queryAssistant };
