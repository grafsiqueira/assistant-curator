const {
  getConversations,
  updateConversation,
  registerLogin,
} = require("../../helpers/helpers.js");

const {
  getCognosSession,
  initializeDashboard,
} = require("../../helpers/cognos");

const { insertOnCloudant, getFromCloudant } = require("../../helpers/cloudant");

async function getLogs(req, res) {
  const conversations = await getConversations(req.body);
  conversations ? res.send({ conversations }) : res.send(null);
}
async function updateScore(req, res) {
  const { conversation, connStr, table } = req.body;
  await updateConversation(conversation, connStr, table);
  res.send({ result: "Conversation Updated" });
}

async function cognosSession(req, res) {
  const { username, password } = req.body;
  res.send(await getCognosSession(username, password));
}

async function insertOnCloudantController(req, res) {
  const { apikey, url, dbName, docId, document } = req.body;
  res.send(await insertOnCloudant(apikey, url, dbName, docId, document));
}

async function getFromCloudantController(req, res) {
  const { apikey, url, dbName, docId } = req.body;
  res.send(await getFromCloudant(apikey, url, dbName, docId));
}

function initializeDashboardController(req, res) {
  const {
    xsd,
    jdbcUrl,
    driverClassName,
    schema,
    user,
    password,
    logsTable,
    conversationTable,
    callsTable,
    contextTable,
    conversationPathTable,
    overviewTable,
    classDistributionTable,
    precisionAtKTable,
    classAccuracyTable,
    pairWiseClassErrorsTable,
    accuracyVsCoverageTable,
  } = req.body;

  res.send(
    initializeDashboard(
      xsd,
      jdbcUrl,
      driverClassName,
      schema,
      user,
      password,
      logsTable,
      conversationTable,
      callsTable,
      contextTable,
      conversationPathTable,
      overviewTable,
      classDistributionTable,
      precisionAtKTable,
      classAccuracyTable,
      pairWiseClassErrorsTable,
      accuracyVsCoverageTable
    )
  );
}

async function registerLoginController(req, res) {
  const { docId, document } = req.body;
  await registerLogin(docId, document);
  res.send("Registered");
}

module.exports = {
  getLogs,
  updateScore,
  cognosSession,
  insertOnCloudantController,
  getFromCloudantController,
  initializeDashboardController,
  registerLoginController,
};
