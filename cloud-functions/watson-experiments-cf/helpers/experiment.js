const Assistant = require("watson-assistant-experiment");

async function runExperiment(assistantCreds) {
  const { version, apiKey, url, workspaceID } = assistantCreds;
  const assistant = new Assistant({
    version: version,
    apikey: apiKey,
    url: url,
  });

  const result = await assistant.runExperiment({
    workspace_id: workspaceID,
  });

  return result;
}

module.exports = {
  runExperiment,
};
