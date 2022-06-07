import { useGlobalState } from "../../hooks/globalState";
import { ToastNotification } from "carbon-components-react";
import textLanguage from "../../helpers/languagesConfig";
import "./style.css";

export default function WarningModal() {
  const { language, setWarningOpen, warningOpen } = useGlobalState();

  function handleClose() {
    setWarningOpen(false);
  }

  const notificationProps = () => ({
    kind: "error",
    title: textLanguage[language].modalWarning.heading,
    subtitle: textLanguage[language].modalWarning.text,
    caption: textLanguage[language].modalWarning.caption,
    iconDescription: "Error",
    statusIconDescription: "Error",
    onClose: handleClose,
    onCloseButtonClick: handleClose,
    timeout: 4500,
  });

  return warningOpen ? (
    <ToastNotification
      {...notificationProps()}
      style={{ marginBottom: ".5rem" }}
    />
  ) : (
    ""
  );
}
