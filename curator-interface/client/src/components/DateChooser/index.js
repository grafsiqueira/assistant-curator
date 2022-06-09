import React, { useState, useEffect } from "react";
import { Pagination, Button } from "carbon-components-react";
import { PageLast32, PageFirst32 } from "@carbon/icons-react";

import Languages from "../../helpers/languagesConfig";
import { useGlobalState } from "../../hooks/globalState";

export default function PaginationComponent() {
  const {
    conversationsByDay,
    dateFilter,
    setDateFilter,
    setCurrentPage,
    language,
  } = useGlobalState();

  const [dateToShow, setDateToShow] = useState("");
  useEffect(() => {
    setDateToShow(dateFilter.split("-").reverse().join("/"));
  }, [dateFilter]);

  function previousDate() {
    setDateFilter(
      Object.keys(conversationsByDay)[
        Object.keys(conversationsByDay).indexOf(dateFilter) - 1
      ]
    );
    selectFirstTab();
    setCurrentPage(1);
  }
  function nextDate() {
    setDateFilter(
      Object.keys(conversationsByDay)[
        Object.keys(conversationsByDay).indexOf(dateFilter) + 1
      ]
    );
    selectFirstTab();
    setCurrentPage(1);
  }
  function selectFirstTab() {
    const firstTab = document.getElementById("firstTab");
    if (firstTab) firstTab.click();
  }
  return (
    <>
      <div id="leftButtons">
        <Button
          kind="ghost"
          hasIconOnly
          iconDescription="Previous Date"
          renderIcon={PageFirst32}
          onClick={() => {
            previousDate();
          }}
          disabled={
            Object.keys(conversationsByDay).indexOf(dateFilter) === 0
              ? true
              : false
          }
        />
      </div>
      <div id="dateText">{`${dateToShow}`}</div>
      <div id="rightButtons">
        <Button
          kind="ghost"
          hasIconOnly
          iconDescription="Next Date"
          renderIcon={PageLast32}
          onClick={() => {
            nextDate();
          }}
          disabled={
            Object.keys(conversationsByDay).indexOf(dateFilter) ===
            Object.keys(conversationsByDay).length - 1
              ? true
              : false
          }
        />
      </div>
    </>
  );
}
