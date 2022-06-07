import { useGlobalState } from "../../hooks/globalState";
import { Modal, TextInput } from "carbon-components-react";
import { sendToCloudant } from "../../helpers/helpers";
import textLanguage from "../../helpers/languagesConfig";
import "./style.css";

export default function SaveModal() {
  const {
    language,
    cloudantApi,
    cloudantUrl,
    cloudantDbName,
    defaultDashboardName,
    setDefaultDashboardName,
    cognosDashboard,
    setUnsavedChanges,
    setWarningOpen,
    saveModalOpen,
    setSaveModalOpen,
  } = useGlobalState();

  async function handleSubmit() {
    setWarningOpen(false);
    const response = await sendToCloudant(
      cloudantApi,
      cloudantUrl,
      cloudantDbName,
      defaultDashboardName,
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
        setDefaultDashboardName(defaultDashboardName);
      }}
      onRequestSubmit={handleSubmit}
      preventCloseOnClickOutside
    >
      <TextInput
        data-modal-primary-focus
        id="text-input-9"
        labelText={textLanguage[language].saveModal.inputLabel1}
        placeholder={defaultDashboardName}
        style={{ marginBottom: "1rem" }}
        onChange={(e) => {
          setDefaultDashboardName(e.target.value);
        }}
      />
    </Modal>
  );
}
