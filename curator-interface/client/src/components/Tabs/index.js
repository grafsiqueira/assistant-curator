import { useGlobalState } from "../../hooks/globalState";
import { Tabs, Tab } from "carbon-components-react";
import InfoTable from "../InfoTable";

export default function BasicTabs() {
  const { conversations, rowData } = useGlobalState();

  return (
    <Tabs scrollIntoView type="container">
      {Object.keys(conversations).map((conversationID) => (
        <Tab id={conversationID} label={conversationID}>
          <InfoTable ID={conversationID} rowData={rowData} />
        </Tab>
      ))}
    </Tabs>
  );
}
