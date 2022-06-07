import { useGlobalState } from "../../hooks/globalState";
import { Modal, TextInput } from "carbon-components-react";
import { getFromCloudant } from "../../helpers/helpers";
import textLanguage from "../../helpers/languagesConfig";
import "./style.css";

export default function LoadModal() {
  const {
    language,
    cloudantApi,
    cloudantUrl,
    cloudantDbName,
    defaultDashboardName,
    setDefaultDashboardName,
    setWarningOpen,
    loadModalOpen,
    setUnsavedChanges,
    setLoadModalOpen,
    cognosClient,
    setCognosDashboard,
  } = useGlobalState();

  async function loadDashboard(cognosApi, dashboard) {
    return new Promise((resolve, reject) => {
      try {
        cognosApi.initialize().then(() => {
          cognosApi.dashboard
            .openDashboard({
              dashboardSpec: dashboard,
            })
            .then(async (dashboardAPI) => {
              dashboardAPI.setMode(dashboardAPI.MODES.EDIT);
              setCognosDashboard(await dashboardAPI.getSpec());
              dashboardAPI.on(dashboardAPI.EVENTS.DIRTY, async () => {
                setUnsavedChanges(true);
                const dashSpec = await dashboardAPI.getSpec();
                setCognosDashboard(dashSpec);
              });
              resolve("Ok");
            });
        });
      } catch (err) {
        reject("Bad Credentials/DbName  ");
      }
    });
  }

  async function handleSubmit() {
    setWarningOpen(false);
    const dashboardSpec = await getFromCloudant(
      defaultDashboardName,
      cloudantApi,
      cloudantUrl,
      cloudantDbName,
      setWarningOpen
    );
    if (dashboardSpec.Error) {
      setLoadModalOpen(false);
    } else {
      await loadDashboard(cognosClient, dashboardSpec);
      setUnsavedChanges(false);
      setLoadModalOpen(false);
    }
  }

  return (
    <Modal
      open={loadModalOpen}
      modalHeading={textLanguage[language].loadModal.heading}
      modalLabel={textLanguage[language].loadModal.label}
      primaryButtonText={textLanguage[language].loadModal.primaryText}
      secondaryButtonText={textLanguage[language].loadModal.secondaryText}
      onRequestClose={() => {
        setWarningOpen(false);
        setLoadModalOpen(false);
        setDefaultDashboardName(defaultDashboardName);
      }}
      onRequestSubmit={handleSubmit}
      preventCloseOnClickOutside
    >
      <TextInput
        data-modal-primary-focus
        id="text-input-9"
        labelText={textLanguage[language].loadModal.inputLabel1}
        placeholder={defaultDashboardName}
        style={{ marginBottom: "1rem" }}
        onChange={(e) => {
          setDefaultDashboardName(e.target.value);
        }}
      />
    </Modal>
  );
}
