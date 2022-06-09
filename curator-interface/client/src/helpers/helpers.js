import BasicRating from "../components/Rating";
import api from "../services/api";

export function createRows(conversations, setRowData) {
  let rowsAux = [];

  Object.entries(conversations).map(([key, value]) => {
    conversations[key].map((log) => {
      let rowAux = {};

      rowAux.id = log.LOGID;
      rowAux.CONVERSATIONID = log.CONVERSATIONID;
      rowAux.LOGID = log.LOGID;
      rowAux.CLIENTMESSAGE = log.CLIENTMESSAGE;
      rowAux.ASSISTANTMESSAGE = log.ASSISTANTMESSAGE;
      rowAux.FIRSTINTENT = log.FIRSTINTENT;
      rowAux.CLIENTTIMESTAMP = log.CLIENTTIMESTAMP;
      rowAux.SCORE = (
        <BasicRating
          conversationID={log.CONVERSATIONID}
          logID={log.LOGID}
          defaultValue={log.SCORE}
        />
      );

      rowsAux.push(rowAux);
    });
  });
  setRowData(rowsAux);
}

export async function getLogs(
  connectionString,
  tableName,
  setConversations,
  setConversationsByDay,
  setIntentsByDay,
  setDateFilter,
  setSuccessOpen,
  setLoading,
  callback,
  param
) {
  const response = await api.post("/getLogs", {
    connStr: connectionString,
    table: tableName,
  });
  if (
    response.data.conversations &&
    Object.keys(response.data.conversations).length != 0
  ) {
    setSuccessOpen(true);
    setConversations(response.data.conversations);
    setDateFilter(Object.keys(response.data.conversationsByDay)[0]);
    setConversationsByDay(response.data.conversationsByDay);
    setIntentsByDay(response.data.intentsByDay);
    setLoading(false);
  } else {
    callback(param);
  }
}

export async function sendScore(
  conversations,
  connectionString,
  tableName,
  setConversations
) {
  await api.post("/updateScore", {
    conversation: conversations,
    connStr: connectionString,
    table: tableName,
  });

  let updateRender = [];
  Object.entries(conversations).map(([key, value]) => {
    updateRender.push([key, value.filter((obj) => obj.SCORE === null)]);
  });
  updateRender = updateRender.filter(([key, value]) => value.length !== 0);

  setConversations(Object.fromEntries(updateRender));
}

export async function getCognosSession(username, password) {
  return await api.post("/cognosSession", {
    username: username,
    password: password,
  });
}

export async function sendToCloudant(
  cloudantApi,
  cloudantUrl,
  cloudantDbName,
  dashboardName,
  cognosDashboard,
  setWarningOpen
) {
  const response = await api.post("/insertDashboard", {
    apikey: cloudantApi,
    url: cloudantUrl,
    dbName: cloudantDbName,
    docId: dashboardName,
    document: cognosDashboard,
  });
  if (response.data.Error) {
    setWarningOpen(true);
    return response.data;
  } else {
    return response.data;
  }
}

export async function getFromCloudant(
  ID,
  cloudantApi,
  cloudantUrl,
  cloudantDbName,
  setWarningOpen
) {
  const response = await api.post("/getDashboard", {
    apikey: cloudantApi,
    url: cloudantUrl,
    dbName: cloudantDbName,
    docId: ID,
  });

  if (response.data.Error) {
    setWarningOpen(true);
    return response.data;
  } else {
    return response.data;
  }
}

export async function initializeSources(
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
) {
  const response = await api.post("/initializeDashboard", {
    xsd: xsd,
    jdbcUrl: jdbcUrl,
    driverClassName: driverClassName,
    schema: schema,
    user: user,
    password: password,
    logsTable: logsTable,
    conversationTable: conversationTable,
    callsTable: callsTable,
    contextTable: contextTable,
    conversationPathTable: conversationPathTable,
    overviewTable: overviewTable,
    classDistributionTable: classDistributionTable,
    precisionAtKTable: precisionAtKTable,
    classAccuracyTable: classAccuracyTable,
    pairWiseClassErrorsTable: pairWiseClassErrorsTable,
    accuracyVsCoverageTable: accuracyVsCoverageTable,
  });

  delete response.data._id;
  delete response.data._rev;

  return response.data;
}

export function generateConnectionString(credential) {
  const credentials = credential.credentials.connection.db2;
  return `DATABASE=${credentials.database};HOSTNAME=${credentials.hosts[0].hostname};PORT=${credentials.hosts[0].port};PROTOCOL=TCPIP;UID=${credentials.authentication.username};PWD=${credentials.authentication.password};SECURITY=SSL;`;
}

export function registerLogin(docId, document) {
  return new Promise(async (resolve, reject) => {
    try {
      resolve(
        await api.post("/registerLogin", { docId: docId, document: document })
      );
    } catch (err) {
      console.log(err);
    }
  });
}

export const defaultValues = {
  credentials: {
    connectionString: null,
    db2Username: null,
    db2Password: null,
    jdbcUrl: null,
    cognosUsername: null,
    cognosPassword: null,
    cloudantApi: null,
    cloudantUrl: null,
  },
  defaults: {
    defaultDashboardName: null,
    cloudantDbName: "dashboards",
    xsd: "https://ibm.com/daas/module/1.0/module.xsd",
    driverClassName: "com.ibm.db2.jcc.DB2Driver",
    schema: "CURATOR",
    logsTable: "LOGS",
    conversationTable: "CONVERSATIONS",
    callsTable: "CALLS",
    contextTable: "CONTEXTVARIABLES",
    conversationPathTable: "CONVERSATIONPATH",
    overviewTable: "OVERVIEW",
    classDistributionTable: "CLASSDISTRIBUTION",
    precisionAtKTable: "PRECISIONATK",
    classAccuracyTable: "CLASSACCURACY",
    pairWiseClassErrorsTable: "PAIRWISECLASSERRORS",
    accuracyVsCoverageTable: "ACCURACYVSCOVERAGE",
  },
};
