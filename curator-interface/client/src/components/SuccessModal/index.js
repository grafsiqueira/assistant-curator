import { useGlobalState } from "../../hooks/globalState";
import { ToastNotification } from "carbon-components-react";
import textLanguage from "../../helpers/languagesConfig";
import "./style.css";

export default function SuccessModal() {
  const { language, setSuccessOpen, successOpen } = useGlobalState();

  function handleClose() {
    setSuccessOpen(false);
  }

  const notificationProps = () => ({
    kind: "success",
    title: textLanguage[language].modalSuccess.heading,
    subtitle: textLanguage[language].modalSuccess.text,
    caption: textLanguage[language].modalSuccess.caption,
    iconDescription: "Success",
    statusIconDescription: "Success",
    onClose: handleClose,
    onCloseButtonClick: handleClose,
    timeout: 4500,
  });

  return successOpen ? (
    <ToastNotification
      {...notificationProps()}
      style={{ marginBottom: ".5rem" }}
    />
  ) : (
    ""
  );
}
