import { useGlobalState } from "../../hooks/globalState";
import React, { useEffect, useState } from "react";
import { Modal, Tabs, Tab, Loading } from "carbon-components-react";
import TextInput from "../TextInput";
import Select from "../Select";
import { getCognosSession } from "../../helpers/helpers";
import { getResourceKeys } from "../../helpers/apiCalls";

import textLanguage from "../../helpers/languagesConfig";
import "./style.css";

export default function CognosConfigModal() {
  const {
    history,
    language,
    cognosConfigOpen,
    setCognosConfigOpen,
    credentialsAndDefaults,
    setCredentialsAndDefaults,
    setCognosSession,
    setWarningOpen,
    setUnsavedChanges,
    accountLoading,
    setCognosDashboard,
  } = useGlobalState();

  const [cognosInstance, setCognosInstance] = useState(null);
  const [cloudantInstance, setCloudantInstance] = useState(null);
  const [db2Instance, setDb2Instance] = useState(null);

  useEffect(() => {
    if (cognosInstance) handleCognosCredentials();
  }, [cognosInstance]);

  useEffect(() => {
    if (cloudantInstance) handleCloudantCredentials();
  }, [cloudantInstance]);

  useEffect(() => {
    if (db2Instance) handleDb2Credentials();
  }, [db2Instance]);

  async function handleCognosCredentials() {
    const resourceKeys = await getResourceKeys(cognosInstance.guid).catch(() =>
      history.push("/login")
    );
    const credentials = resourceKeys.resources[0].credentials;
    let temp = { ...credentialsAndDefaults };
    temp.credentials.cognosUsername = credentials.client_id;
    temp.credentials.cognosPassword = credentials.client_secret;
    setCredentialsAndDefaults(temp);
  }

  async function handleCloudantCredentials() {
    const resourceKeys = await getResourceKeys(cloudantInstance.guid).catch(
      () => history.push("/login")
    );
    const credentials = resourceKeys.resources[0].credentials;
    let temp = { ...credentialsAndDefaults };
    temp.credentials.cloudantApi = credentials.apikey;
    temp.credentials.cloudantUrl = credentials.url;
    setCredentialsAndDefaults(temp);
  }

  async function handleDb2Credentials() {
    const resourceKeys = await getResourceKeys(db2Instance.guid).catch(() =>
      history.push("/login")
    );
    const credentials = resourceKeys.resources[0].credentials;
    let temp = { ...credentialsAndDefaults };
    temp.credentials.db2Username =
      credentials.connection.db2.authentication.username;
    temp.credentials.db2Password =
      credentials.connection.db2.authentication.password;
    temp.credentials.jdbcUrl = credentials.connection.db2.jdbc_url[0].replace(
      "user=<userid>;password=<your_password>;",
      ""
    );
    setCredentialsAndDefaults(temp);
  }

  async function handleSubmit() {
    setCognosSession(null);
    setCognosDashboard(null);
    setUnsavedChanges(false);
    const response = await getCognosSession(
      credentialsAndDefaults.credentials.cognosUsername,
      credentialsAndDefaults.credentials.cognosPassword
    );
    if (response.data.Error) {
      setWarningOpen(true);
      setCognosSession(null);
    } else {
      setCognosSession(response.data);
    }
  }

  return !accountLoading ? (
    <Modal
      id="cognosModal"
      hasForm
      open={cognosConfigOpen}
      modalHeading={textLanguage[language].cognosModal.heading}
      modalLabel={textLanguage[language].cognosModal.label}
      primaryButtonText={textLanguage[language].cognosModal.primaryText}
      secondaryButtonText={textLanguage[language].cognosModal.secondaryText}
      onRequestClose={() => {
        setCognosConfigOpen(false);
      }}
      onRequestSubmit={handleSubmit}
      preventCloseOnClickOutside
    >
      <Tabs type="container">
        <Tab id={"cognos"} label={textLanguage[language].cognosModal.tab1}>
          <Select index={1} setSelection={setCognosInstance} field="cognos" />
        </Tab>
        <Tab id={"cloudant"} label={textLanguage[language].cognosModal.tab2}>
          <Select
            index={2}
            setSelection={setCloudantInstance}
            field="cloudant"
          />
          <TextInput index={3} field="cloudantDbName" upperCase={false} />
        </Tab>
        <Tab id={"db2"} label={textLanguage[language].cognosModal.tab3}>
          <Select index={2} setSelection={setDb2Instance} field="db2" />

          <TextInput index={5} field="xsd" upperCase={false} />
          <TextInput index={6} field="driverClassName" upperCase={false} />
        </Tab>
        <Tab id="db2tables" label={textLanguage[language].cognosModal.tab4}>
          <TextInput index={7} field="logsTable" upperCase={true} />
          <TextInput index={8} field="conversationTable" upperCase={true} />
          <TextInput index={9} field="callsTable" upperCase={true} />
          <TextInput index={10} field="contextTable" upperCase={true} />
          <TextInput
            index={11}
            field="conversationPathTable"
            upperCase={true}
          />
          <TextInput index={12} field="overviewTable" upperCase={true} />
          <TextInput
            index={13}
            field="classDistributionTable"
            upperCase={true}
          />
          <TextInput index={14} field="precisionAtKTable" upperCase={true} />
          <TextInput index={15} field="classAccuracyTable" upperCase={true} />
          <TextInput
            index={16}
            field="pairWiseClassErrorsTable"
            upperCase={true}
          />
          <TextInput
            index={17}
            field="accuracyVsCoverageTable"
            upperCase={true}
          />
        </Tab>
      </Tabs>
    </Modal>
  ) : (
    <Loading />
  );
}
