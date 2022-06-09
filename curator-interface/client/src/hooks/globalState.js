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
  const [conversationsByDay, setConversationsByDay] = useState({});
  const [intentsByDay, setIntentsByDay] = useState({});
  const [dateFilter, setDateFilter] = useState("");

  const [cognosClient, setCognosClient] = useState(null);
  const [cognosSession, setCognosSession] = useState({});
  const [cognosDashboard, setCognosDashboard] = useState(null);

  const [credentialsAndDefaults, setCredentialsAndDefaults] = useState(
    JSON.parse(
      localStorage.getItem("@assistant-curator/credentialsAndDefaults")
    )
  );

  // pagination
  const [itensPerPage, setItensPerPage] = useState(9);
  const [currentPage, setCurrentPage] = useState(1);
  const startIndex = (currentPage - 1) * itensPerPage;
  const endIndex = startIndex + itensPerPage;

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

  useEffect(() => {
    localStorage.setItem(
      "@assistant-curator/credentialsAndDefaults",
      JSON.stringify(credentialsAndDefaults)
    );
  }, [credentialsAndDefaults]);

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
        conversationsByDay,
        setConversationsByDay,
        intentsByDay,
        setIntentsByDay,
        dateFilter,
        setDateFilter,
        cognosClient,
        setCognosClient,
        cognosSession,
        setCognosSession,
        cognosDashboard,
        setCognosDashboard,
        credentialsAndDefaults,
        setCredentialsAndDefaults,
        history,
        accounts,
        setAccounts,
        account,
        setAccount,
        resources,
        setResources,
        itensPerPage,
        setItensPerPage,
        currentPage,
        setCurrentPage,
        startIndex,
        endIndex,
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
