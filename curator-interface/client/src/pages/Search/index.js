import React, { useEffect, useState } from "react";
import "carbon-components/css/carbon-components.min.css";
import Header from "../../components/Header";
import { useGlobalState } from "../../hooks/globalState";

import {
  Accordion,
  AccordionItem,
  AccordionSkeleton,
  Search,
} from "carbon-components-react";
import InfoTable from "../../components/InfoTable";
import { getLogs, groupByIntent, createRows } from "../../helpers/helpers";

import ConfigModal from "../../components/ConfigModal";

import "./style.css";

export default function SearchPage() {
  const {
    account,
    history,
    setLanguage,
    loading,
    setLoading,
    setHelpOpen,
    setConfigOpen,
    setSuccessOpen,
    setWarningOpen,
    conversations,
    rowData,
    setRowData,
    setConversations,
    connectionString,
    logsTable,
    setCognosSession,
  } = useGlobalState();

  const [intentGroups, setIntentGroups] = useState([]);
  const [searchValue, setSearchValue] = useState("");

  useEffect(() => {
    if (account.entity.name === "") history.push("/login");
  }, []);

  useEffect(async () => {
    const storedLanguage = localStorage.getItem("language");
    if (storedLanguage) setLanguage(storedLanguage);

    setSuccessOpen(false);
    setWarningOpen(false);
    setCognosSession(null);

    if (conversations.length === 0)
      await getLogs(
        connectionString,
        logsTable,
        setConversations,
        setSuccessOpen,
        setLoading,
        setConfigOpen,
        true
      );
  }, []);

  useEffect(() => {
    setIntentGroups(groupByIntent(conversations).sort());
    createRows(conversations, setRowData);
  }, [conversations]);

  const props = () => ({
    className: "some-class",
    size: "xl",
    light: false,
    disabled: false,
    defaultValue: "",
    labelText: "Search",
    placeholder: "Search",
  });

  return (
    <div id="content">
      <Header modalOpen={setConfigOpen} helpOpen={setHelpOpen} />
      <ConfigModal />
      <Search
        {...props()}
        id="searchBar"
        onChange={(e) => {
          setSearchValue(e.target.value);
        }}
      />
      {loading ? (
        <AccordionSkeleton />
      ) : (
        <div id="accords">
          <Accordion>
            {intentGroups.map((intent) =>
              intent
                .toLowerCase()
                .replace(/\s/g, "")
                .includes(searchValue.toLowerCase().replace(/\s/g, "")) ? (
                <AccordionItem title={intent === "" ? "-" : intent}>
                  <InfoTable ID={intent} rowData={rowData} />
                </AccordionItem>
              ) : (
                ""
              )
            )}
          </Accordion>
        </div>
      )}
    </div>
  );
}
