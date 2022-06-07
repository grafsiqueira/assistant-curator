import { useGlobalState } from "../../hooks/globalState";
import React, { useEffect, useState } from "react";
import {
  Modal,
  TextInput,
  Tabs,
  Tab,
  Select,
  SelectItem,
  Loading,
} from "carbon-components-react";

import { getCognosSession } from "../../helpers/helpers";
import { getResourceKeys } from "../../helpers/apiCalls";

import textLanguage from "../../helpers/languagesConfig";
import "./style.css";

export default function CognosConfigModal() {
  const {
    history,
    resources,
    language,
    cognosConfigOpen,
    setCognosConfigOpen,
    cognosUsername,
    setCognosUsername,
    cognosPassword,
    setCognosPassword,
    setCloudantApi,
    setCloudantUrl,
    cloudantDbName,
    setCloudantDbName,
    setCognosSession,
    setCognosDashboard,
    setWarningOpen,
    setUnsavedChanges,
    xsd,
    setXsd,
    setJdbcUrl,
    driverClassName,
    setDriverClassName,
    setDb2UserName,
    setDb2Passwor,
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
    accountLoading,
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
    setCognosUsername(credentials.client_id);
    setCognosPassword(credentials.client_secret);
  }

  async function handleCloudantCredentials() {
    const resourceKeys = await getResourceKeys(cloudantInstance.guid).catch(
      () => history.push("/login")
    );
    const credentials = resourceKeys.resources[0].credentials;
    setCloudantApi(credentials.apikey);
    setCloudantUrl(credentials.url);
  }

  async function handleDb2Credentials() {
    const resourceKeys = await getResourceKeys(db2Instance.guid).catch(() =>
      history.push("/login")
    );
    const credentials = resourceKeys.resources[0].credentials;

    setDb2UserName(credentials.connection.db2.authentication.username);
    setDb2Passwor(credentials.connection.db2.authentication.password);
    setJdbcUrl(
      credentials.connection.db2.jdbc_url[0].replace(
        "user=<userid>;password=<your_password>;",
        ""
      )
    );
  }

  async function handleSubmit() {
    setCognosSession(null);
    setCognosDashboard(null);
    setUnsavedChanges(false);
    const response = await getCognosSession(cognosUsername, cognosPassword);
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
          <Select
            id="select-1"
            invalidText="A valid value is required"
            labelText={textLanguage[language].cognosModal.inputLabel1}
            onChange={(e) => {
              if (e.target.value === "placeholder")
                alert(textLanguage[language].cognosModal.alert);
              else setCognosInstance(JSON.parse(e.target.value));
            }}
          >
            <SelectItem
              text={textLanguage[language].cognosModal.selectAnOption}
              value="placeholder"
            />
            {resources.cognos.body.resources.map((resource) => (
              <SelectItem
                text={resource.name}
                value={JSON.stringify(resource)}
              />
            ))}
          </Select>
        </Tab>
        <Tab id={"cloudant"} label={textLanguage[language].cognosModal.tab2}>
          <Select
            id="select-1"
            invalidText="A valid value is required"
            labelText={textLanguage[language].cognosModal.inputLabel2}
            onChange={(e) => {
              if (e.target.value === "placeholder")
                alert(textLanguage[language].cognosModal.alert);
              else setCloudantInstance(JSON.parse(e.target.value));
            }}
          >
            <SelectItem
              text={textLanguage[language].cognosModal.selectAnOption}
              value="placeholder"
            />
            {resources.cloudant.body.resources.map((resource) => (
              <SelectItem
                text={resource.name}
                value={JSON.stringify(resource)}
              />
            ))}
          </Select>

          <TextInput
            data-modal-primary-focus
            id="text-input-7"
            labelText={textLanguage[language].cognosModal.inputLabel3}
            placeholder={cloudantDbName}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setCloudantDbName(e.target.value);
            }}
          />
        </Tab>
        <Tab id={"db2"} label={textLanguage[language].cognosModal.tab3}>
          <Select
            id="select-1"
            invalidText="A valid value is required"
            labelText={textLanguage[language].cognosModal.inputLabel4}
            onChange={(e) => {
              if (e.target.value === "placeholder")
                alert(textLanguage[language].cognosModal.alert);
              else setDb2Instance(JSON.parse(e.target.value));
            }}
          >
            <SelectItem
              text={textLanguage[language].cognosModal.selectAnOption}
              value="placeholder"
            />
            {resources.db2.body.resources.map((resource) => (
              <SelectItem
                text={resource.name}
                value={JSON.stringify(resource)}
              />
            ))}
          </Select>

          <TextInput
            data-modal-primary-focus
            id="text-input-8"
            labelText={textLanguage[language].cognosModal.inputLabel5}
            placeholder={xsd}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setXsd(e.target.value);
            }}
          />

          <TextInput
            data-modal-primary-focus
            id="text-input-10"
            labelText={textLanguage[language].cognosModal.inputLabel6}
            placeholder={driverClassName}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setDriverClassName(e.target.value);
            }}
          />
        </Tab>
        <Tab id="db2tables" label={textLanguage[language].cognosModal.tab4}>
          <TextInput
            data-modal-primary-focus
            id="text-input-14"
            labelText={textLanguage[language].cognosModal.inputLabel7}
            placeholder={logsTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setLogsTable(e.target.value.toUpperCase());
            }}
          />
          <TextInput
            data-modal-primary-focus
            id="text-input-15"
            labelText={textLanguage[language].cognosModal.inputLabel8}
            placeholder={conversationTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setConversationTable(e.target.value.toUpperCase());
            }}
          />
          <TextInput
            data-modal-primary-focus
            id="text-input-16"
            labelText={textLanguage[language].cognosModal.inputLabel9}
            placeholder={callsTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setCallsTable(e.target.value.toUpperCase());
            }}
          />
          <TextInput
            data-modal-primary-focus
            id="text-input-17"
            labelText={textLanguage[language].cognosModal.inputLabel10}
            placeholder={contextTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setContextTable(e.target.value.toUpperCase());
            }}
          />
          <TextInput
            data-modal-primary-focus
            id="text-input-17"
            labelText={textLanguage[language].cognosModal.inputLabel11}
            placeholder={conversationPathTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setConversationPathTable(e.target.value.toUpperCase());
            }}
          />
          <TextInput
            data-modal-primary-focus
            id="text-input-17"
            labelText={textLanguage[language].cognosModal.inputLabel12}
            placeholder={overviewTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setOverviewTable(e.target.value.toUpperCase());
            }}
          />
          <TextInput
            data-modal-primary-focus
            id="text-input-17"
            labelText={textLanguage[language].cognosModal.inputLabel13}
            placeholder={classDistributionTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setClassDistributionTable(e.target.value.toUpperCase());
            }}
          />
          <TextInput
            data-modal-primary-focus
            id="text-input-17"
            labelText={textLanguage[language].cognosModal.inputLabel14}
            placeholder={precisionAtKTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setPrecisionAtKTable(e.target.value.toUpperCase());
            }}
          />
          <TextInput
            data-modal-primary-focus
            id="text-input-17"
            labelText={textLanguage[language].cognosModal.inputLabel15}
            placeholder={classAccuracyTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setClassAccuracyTable(e.target.value.toUpperCase());
            }}
          />
          <TextInput
            data-modal-primary-focus
            id="text-input-17"
            labelText={textLanguage[language].cognosModal.inputLabel16}
            placeholder={pairWiseClassErrorsTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setPairWiseClassErrorsTable(e.target.value.toUpperCase());
            }}
          />
          <TextInput
            data-modal-primary-focus
            id="text-input-17"
            labelText={textLanguage[language].cognosModal.inputLabel17}
            placeholder={accuracyVsCoverageTable}
            style={{ marginBottom: "1rem" }}
            onChange={(e) => {
              setAccuracyVsCoverageTable(e.target.value.toUpperCase());
            }}
          />
        </Tab>
      </Tabs>
    </Modal>
  ) : (
    <Loading />
  );
}
