import { useGlobalState } from "../../hooks/globalState";
import { ToastNotification } from "carbon-components-react";
import textLanguage from "../../helpers/languagesConfig";
import "./style.css";

export default function StandardDashboardModal() {
  const { language, setStandardDashboardModal, standardDashboardModal } =
    useGlobalState();

  function handleClose() {
    setStandardDashboardModal(false);
  }

  const notificationProps = () => ({
    kind: "warning",
    title: textLanguage[language].standardDashboardModal.heading,
    subtitle: textLanguage[language].standardDashboardModal.text,
    caption: textLanguage[language].standardDashboardModal.caption,
    iconDescription: "Warning",
    statusIconDescription: "Warning",
    onClose: handleClose,
    onCloseButtonClick: handleClose,
    timeout: 4500,
  });

  return standardDashboardModal ? (
    <ToastNotification
      {...notificationProps()}
      style={{ marginBottom: ".5rem" }}
    />
  ) : (
    ""
  );
}
