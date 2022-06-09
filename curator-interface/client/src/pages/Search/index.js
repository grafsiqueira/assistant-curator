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
import DateChooser from "../../components/DateChooser";

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
    setConversations,
    setConversationsByDay,
    intentsByDay,
    setIntentsByDay,
    dateFilter,
    setDateFilter,
    rowData,
    setRowData,
    credentialsAndDefaults,
    setCognosSession,
  } = useGlobalState();

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
        credentialsAndDefaults.credentials.connectionString,
        credentialsAndDefaults.defaults.logsTable,
        setConversations,
        setConversationsByDay,
        setIntentsByDay,
        setDateFilter,
        setSuccessOpen,
        setLoading,
        setConfigOpen,
        true
      );
  }, []);

  useEffect(() => {
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
    <div id="search-page">
      <div id="top">
        <Header
          id="header-search"
          modalOpen={setConfigOpen}
          helpOpen={setHelpOpen}
        />
        <div id="dateChooser">
          <DateChooser />
        </div>
        <Search
          {...props()}
          id="searchBar"
          onChange={(e) => {
            if (!e.target.value) {
              setSearchValue("");
            } else {
              setSearchValue(e.target.value);
            }
          }}
        />
      </div>
      <ConfigModal />

      {loading ? (
        <AccordionSkeleton />
      ) : (
        <div id="accords">
          <Accordion>
            {intentsByDay[dateFilter].map((intent) =>
              intent
                .toLowerCase()
                .replace(/\s/g, "")
                .includes(searchValue.toLowerCase().replace(/\s/g, "")) ? (
                <AccordionItem title={intent === "" ? "-" : intent}>
                  <InfoTable ID={intent} day={dateFilter} rowData={rowData} />
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
