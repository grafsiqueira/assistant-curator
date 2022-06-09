import { useGlobalState } from "../../hooks/globalState";
import { Modal, TextInput } from "carbon-components-react";
import { sendToCloudant } from "../../helpers/helpers";
import textLanguage from "../../helpers/languagesConfig";
import "./style.css";

export default function SaveModal() {
  const {
    language,
    credentialsAndDefaults,
    setCredentialsAndDefaults,
    cognosDashboard,
    setUnsavedChanges,
    setWarningOpen,
    saveModalOpen,
    setSaveModalOpen,
  } = useGlobalState();

  async function handleSubmit() {
    setWarningOpen(false);
    const response = await sendToCloudant(
      credentialsAndDefaults.credentials.cloudantApi,
      credentialsAndDefaults.credentials.cloudantUrl,
      credentialsAndDefaults.defaults.cloudantDbName,
      credentialsAndDefaults.defaults.defaultDashboardName,
      cognosDashboard,
      setWarningOpen
    );
    if (!response.Error) {
      setUnsavedChanges(false);
    }
    setSaveModalOpen(false);
  }

  return (
    <Modal
      open={saveModalOpen}
      modalHeading={textLanguage[language].saveModal.heading}
      modalLabel={textLanguage[language].saveModal.label}
      primaryButtonText={textLanguage[language].saveModal.primaryText}
      secondaryButtonText={textLanguage[language].saveModal.secondaryText}
      onRequestClose={() => {
        setWarningOpen(false);
        setSaveModalOpen(false);
      }}
      onRequestSubmit={handleSubmit}
      preventCloseOnClickOutside
    >
      <TextInput
        data-modal-primary-focus
        id="text-input-9"
        labelText={textLanguage[language].saveModal.inputLabel1}
        placeholder={credentialsAndDefaults.defaults.defaultDashboardName}
        style={{ marginBottom: "1rem" }}
        onChange={(e) => {
          let temp = { ...credentialsAndDefaults };
          temp.defaults.defaultDashboardName = e.target.value;
          setCredentialsAndDefaults(temp);
        }}
      />
    </Modal>
  );
}
