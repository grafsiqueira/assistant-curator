import { useGlobalState } from "../../hooks/globalState";
import { Tabs, Tab } from "carbon-components-react";
import InfoTable from "../InfoTable";
import { useEffect, useState } from "react";

export default function BasicTabs() {
  const { rowData, conversationsByDay, dateFilter, startIndex, endIndex } =
    useGlobalState();

  function createLabel(rowData, conversationID) {
    if (rowData.length > 0) {
      const foundConversation = rowData.filter(
        (row) => row.CONVERSATIONID === conversationID
      );
      if (foundConversation.length > 0) {
        const timeStamp = foundConversation[0].CLIENTTIMESTAMP.split(" ");
        return timeStamp[1].slice(0, 8);
      }
    }
  }

  return (
    <>
      <Tabs type="container">
        {conversationsByDay[dateFilter]
          .slice(startIndex, endIndex)
          .map((conversationID) => {
            const label = createLabel(rowData, conversationID);
            if (
              Object.values(
                conversationsByDay[dateFilter].slice(startIndex, endIndex)
              ).indexOf(conversationID) === 0
            ) {
              return (
                <Tab id={"firstTab"} label={label ? label : "DONE!"}>
                  <InfoTable ID={conversationID} rowData={rowData} />
                </Tab>
              );
            } else {
              return (
                <Tab id={conversationID} label={label ? label : "DONE!"}>
                  <InfoTable ID={conversationID} rowData={rowData} />
                </Tab>
              );
            }
          })}
      </Tabs>
    </>
  );
}
