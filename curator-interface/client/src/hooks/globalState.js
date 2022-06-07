import React, { useContext, useState, useEffect, createContext } from "react";
import { useHistory } from "react-router-dom";

import { getAccounts, getResources } from "../helpers/apiCalls";
const GlobalStateContext = createContext({});

export default function GlobalStateProvider({ children }) {
  const history = useHistory();

  const [language, setLanguage] = useState("en");

  const [accounts, setAccounts] = useState({ resources: [] });
  const [account, setAccount] = useState({
    entity: {
      name: "",
    },
  });
  const [resources, setResources] = useState({
    cognos: {
      body: {
        resources: [
          {
            guid: "",
            name: "",
          },
        ],
      },
    },
    cloudant: {
      body: {
        resources: [
          {
            guid: "",
            name: "",
          },
        ],
      },
    },
    db2: {
      body: {
        resources: [
          {
            guid: "",
            name: "",
          },
        ],
      },
    },
  });

  const [loading, setLoading] = useState(true);

  const [helpOpen, setHelpOpen] = useState(false);
  const [cognosHelpOpen, setCognosHelpOpen] = useState(false);

  const [configOpen, setConfigOpen] = useState(false);
  const [cognosConfigOpen, setCognosConfigOpen] = useState(false);
  const [accountModalOpen, setAccountModalOpen] = useState(false);
  const [accountLoading, setAccountLoading] = useState(false);

  const [accountSelected, setAccountSelected] = useState(false);

  const [saveModalOpen, setSaveModalOpen] = useState(false);
  const [loadModalOpen, setLoadModalOpen] = useState(false);

  const [warningOpen, setWarningOpen] = useState(false);
  const [standardDashboardModal, setStandardDashboardModal] = useState(false);
  const [successOpen, setSuccessOpen] = useState(false);

  const [unsavedChanges, setUnsavedChanges] = useState(false);

  const [rowData, setRowData] = useState([]);
  const [conversations, setConversations] = useState([]);

  const [cognosClient, setCognosClient] = useState(null);
  const [cognosSession, setCognosSession] = useState({});
  const [cognosDashboard, setCognosDashboard] = useState(null);

  const [connectionString, setConnectionString] = useState(
    localStorage.getItem("connectionString")
  );

  const [cognosUsername, setCognosUsername] = useState(
    localStorage.getItem("cognosUsername")
  );
  const [cognosPassword, setCognosPassword] = useState(
    localStorage.getItem("cognosPassword")
  );
  const [defaultDashboardName, setDefaultDashboardName] = useState(
    localStorage.getItem("defaultDashboardName")
  );

  const [cloudantApi, setCloudantApi] = useState(
    localStorage.getItem("cloudantApi")
  );
  const [cloudantUrl, setCloudantUrl] = useState(
    localStorage.getItem("cloudantUrl")
  );
  const [cloudantDbName, setCloudantDbName] = useState("dashboards");

  const [xsd, setXsd] = useState("https://ibm.com/daas/module/1.0/module.xsd");

  const [jdbcUrl, setJdbcUrl] = useState(localStorage.getItem("jdbcUrl"));

  const [driverClassName, setDriverClassName] = useState(
    "com.ibm.db2.jcc.DB2Driver"
  );
  const [schema, setSchema] = useState("CURATOR");
  const [db2Username, setDb2UserName] = useState(
    localStorage.getItem("db2Username")
  );
  const [db2Password, setDb2Passwor] = useState(
    localStorage.getItem("db2Password")
  );

  const [logsTable, setLogsTable] = useState("LOGS");
  const [conversationTable, setConversationTable] = useState("CONVERSATIONS");
  const [callsTable, setCallsTable] = useState("CALLS");
  const [contextTable, setContextTable] = useState("CONTEXTVARIABLES");
  const [conversationPathTable, setConversationPathTable] =
    useState("CONVERSATIONPATH");
  const [overviewTable, setOverviewTable] = useState("OVERVIEW");
  const [classDistributionTable, setClassDistributionTable] =
    useState("CLASSDISTRIBUTION");
  const [precisionAtKTable, setPrecisionAtKTable] = useState("PRECISIONATK");
  const [classAccuracyTable, setClassAccuracyTable] = useState("CLASSACCURACY");
  const [pairWiseClassErrorsTable, setPairWiseClassErrorsTable] = useState(
    "PAIRWISECLASSERRORS"
  );
  const [accuracyVsCoverageTable, setAccuracyVsCoverageTable] =
    useState("ACCURACYVSCOVERAGE");

  useEffect(() => {
    localStorage.setItem("connectionString", connectionString);
  }, [connectionString]);

  useEffect(() => {
    localStorage.setItem("cognosUsername", cognosUsername);
  }, [cognosUsername]);
  useEffect(() => {
    localStorage.setItem("cognosPassword", cognosPassword);
  }, [cognosPassword]);

  useEffect(() => {
    localStorage.setItem("cloudantApi", cloudantApi);
  }, [cloudantApi]);
  useEffect(() => {
    localStorage.setItem("cloudantUrl", cloudantUrl);
  }, [cloudantUrl]);
  useEffect(() => {
    if (cloudantDbName != "dashboards")
      localStorage.setItem("cloudantDbName", cloudantDbName);
  }, [cloudantDbName]);
  useEffect(() => {
    localStorage.setItem("defaultDashboardName", defaultDashboardName);
  }, [defaultDashboardName]);

  useEffect(() => {
    if (xsd != "https://ibm.com/daas/module/1.0/module.xsd")
      localStorage.setItem("xsd", xsd);
  }, [xsd]);
  useEffect(() => {
    localStorage.setItem("jdbcUrl", jdbcUrl);
  }, [jdbcUrl]);
  useEffect(() => {
    if (driverClassName != "com.ibm.db2.jcc.DB2Driver")
      localStorage.setItem("driverClassName", driverClassName);
  }, [driverClassName]);
  useEffect(() => {
    localStorage.setItem("db2Username", db2Username);
  }, [db2Username]);
  useEffect(() => {
    localStorage.setItem("db2Password", db2Password);
  }, [db2Password]);

  //tables
  useEffect(() => {
    if (logsTable != "LOGS") localStorage.setItem("logsTable", logsTable);
  }, [logsTable]);
  useEffect(() => {
    if (conversationTable != "CONVERSATIONS")
      localStorage.setItem("conversationTable", conversationTable);
  }, [conversationTable]);
  useEffect(() => {
    if (callsTable != "CALLS") localStorage.setItem("callsTable", callsTable);
  }, [callsTable]);
  useEffect(() => {
    if (contextTable != "CONTEXTVARIABLES")
      localStorage.setItem("contextTable", contextTable);
  }, [contextTable]);
  useEffect(() => {
    if (conversationPathTable != "CONVERSATIONPATH")
      localStorage.setItem("conversationPathTable", conversationPathTable);
  }, [conversationPathTable]);
  useEffect(() => {
    if (overviewTable != "OVERVIEW")
      localStorage.setItem("overviewTable", overviewTable);
  }, [overviewTable]);
  useEffect(() => {
    if (classDistributionTable != "CLASSDISTRIBUTION")
      localStorage.setItem("classDistributionTable", classDistributionTable);
  }, [classDistributionTable]);
  useEffect(() => {
    if (precisionAtKTable != "PRECISIONATK")
      localStorage.setItem("precisionAtKTable", precisionAtKTable);
  }, [precisionAtKTable]);
  useEffect(() => {
    if (classAccuracyTable != "CLASSACCURACY")
      localStorage.setItem("classAccuracyTable", classAccuracyTable);
  }, [classAccuracyTable]);
  useEffect(() => {
    if (pairWiseClassErrorsTable != "PAIRWISECLASSERRORS")
      localStorage.setItem(
        "pairWiseClassErrorsTable",
        pairWiseClassErrorsTable
      );
  }, [pairWiseClassErrorsTable]);
  useEffect(() => {
    if (accuracyVsCoverageTable != "ACCURACYVSCOVERAGE")
      localStorage.setItem("accuracyVsCoverageTable", accuracyVsCoverageTable);
  }, [accuracyVsCoverageTable]);

  useEffect(async () => {
    if (account.entity.name !== "") {
      await getAccounts()
        .then((res) => setAccounts(res))
        .catch((err) => {
          if (err.response.status === 401) history.push("/login");
        });
      await getResources()
        .then((res) => {
          setResources(res);
          setAccountLoading(false);
        })
        .catch((err) => {
          if (err.response.status === 401) history.push("/login");
        });
    }
  }, [account]);

  return (
    <GlobalStateContext.Provider
      value={{
        language,
        setLanguage,
        loading,
        setLoading,
        helpOpen,
        setHelpOpen,
        cognosHelpOpen,
        setCognosHelpOpen,
        saveModalOpen,
        setSaveModalOpen,
        loadModalOpen,
        setLoadModalOpen,
        warningOpen,
        setWarningOpen,
        standardDashboardModal,
        setStandardDashboardModal,
        successOpen,
        setSuccessOpen,
        unsavedChanges,
        setUnsavedChanges,
        configOpen,
        setConfigOpen,
        cognosConfigOpen,
        setCognosConfigOpen,
        accountModalOpen,
        setAccountModalOpen,
        accountSelected,
        setAccountSelected,
        accountLoading,
        setAccountLoading,
        rowData,
        setRowData,
        conversations,
        setConversations,
        cognosClient,
        setCognosClient,
        cognosSession,
        setCognosSession,
        cognosDashboard,
        setCognosDashboard,
        connectionString,
        setConnectionString,
        cognosUsername,
        setCognosUsername,
        cognosPassword,
        setCognosPassword,
        cloudantApi,
        setCloudantApi,
        cloudantUrl,
        setCloudantUrl,
        cloudantDbName,
        setCloudantDbName,
        xsd,
        setXsd,
        jdbcUrl,
        setJdbcUrl,
        driverClassName,
        setDriverClassName,
        schema,
        setSchema,
        db2Username,
        setDb2UserName,
        db2Password,
        setDb2Passwor,
        defaultDashboardName,
        setDefaultDashboardName,
        logsTable,
        setLogsTable,
        conversationTable,
        setConversationTable,
        callsTable,
        setCallsTable,
        contextTable,
        setContextTable,
        conversationPathTable,
        setConversationPathTable,
        overviewTable,
        setOverviewTable,
        classDistributionTable,
        setClassDistributionTable,
        precisionAtKTable,
        setPrecisionAtKTable,
        classAccuracyTable,
        setClassAccuracyTable,
        pairWiseClassErrorsTable,
        setPairWiseClassErrorsTable,
        accuracyVsCoverageTable,
        setAccuracyVsCoverageTable,
        history,
        accounts,
        setAccounts,
        account,
        setAccount,
        resources,
        setResources,
      }}
    >
      {children}
    </GlobalStateContext.Provider>
  );
}

export function useGlobalState() {
  const context = useContext(GlobalStateContext);

  if (!context) {
    throw new Error("useGlobalState must be used within a GlobalStateProvider");
  }

  return context;
}
