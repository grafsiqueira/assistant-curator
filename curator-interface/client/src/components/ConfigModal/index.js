import { useEffect, useState } from "react";
import { useGlobalState } from "../../hooks/globalState";
import {
  Modal,
  TextInput,
  Select,
  SelectItem,
  Loading,
} from "carbon-components-react";
import { getLogs, generateConnectionString } from "../../helpers/helpers";
import textLanguage from "../../helpers/languagesConfig";

import { getResourceKeys } from "../../helpers/apiCalls";

import "./style.css";

export default function ConfigModal() {
  const {
    language,
    configOpen,
    setLoading,
    setConfigOpen,
    setWarningOpen,
    setSuccessOpen,
    setConnectionString,
    logsTable,
    setLogsTable,
    setConversations,
    resources,
    accountLoading,
  } = useGlobalState();

  const [selectedInstance, setSelectedInstance] = useState(null);

  const [selectedCredential, setSelectedCredential] = useState(null);

  useEffect(() => {
    setSelectedInstance(resources.db2.body.resources[0]);
  }, [resources]);

  useEffect(async () => {
    const resourceKeys = await getResourceKeys(selectedInstance.guid);
    setSelectedCredential(resourceKeys.resources[0]);
  }, [selectedInstance]);

  async function handleSubmit() {
    setWarningOpen(false);
    setSuccessOpen(false);
    setConfigOpen(false);
    setLoading(true);

    const connStr = await generateConnectionString(selectedCredential);
    setConnectionString(connStr);
    await getLogs(
      connStr,
      logsTable,
      setConversations,
      setSuccessOpen,
      setLoading,
      noConnection,
      ""
    );
  }

  function noConnection() {
    setConversations({});
    setWarningOpen(true);
    setLoading(false);
  }

  return !accountLoading ? (
    <Modal
      open={configOpen}
      modalHeading={textLanguage[language].modal.heading}
      modalLabel={textLanguage[language].modal.label}
      primaryButtonText={textLanguage[language].modal.primaryText}
      secondaryButtonText={textLanguage[language].modal.secondaryText}
      onRequestClose={() => {
        setConfigOpen(false);
      }}
      onRequestSubmit={handleSubmit}
      preventCloseOnClickOutside
    >
      <Select
        id="select-1"
        invalidText="A valid value is required"
        labelText={textLanguage[language].modal.inputLabel1}
        onChange={(e) => {
          if (e.target.value === "placeholder")
            alert(textLanguage[language].modal.alert);
          else setSelectedInstance(JSON.parse(e.target.value));
        }}
      >
        <SelectItem
          text={textLanguage[language].modal.selectAnOption}
          value="placeholder"
        />
        {resources.db2.body.resources.map((resource) => (
          <SelectItem text={resource.name} value={JSON.stringify(resource)} />
        ))}
      </Select>

      <TextInput
        data-modal-primary-focus
        id="text-input-2"
        labelText={textLanguage[language].modal.inputLabel2}
        placeholder={logsTable}
        style={{ marginBottom: "1rem" }}
        onChange={(e) => {
          setLogsTable(e.target.value.toUpperCase());
        }}
      />
    </Modal>
  ) : (
    <Loading />
  );
}
