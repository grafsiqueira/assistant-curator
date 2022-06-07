import { useGlobalState } from "../../hooks/globalState";
import { Modal } from "carbon-components-react";
import textLanguage from "../../helpers/languagesConfig";
import "./style.css";

export default function HelpModal() {
  const { language, helpOpen, setHelpOpen } = useGlobalState();

  return (
    <Modal
      open={helpOpen}
      modalHeading={textLanguage[language].modalHelp.heading}
      onRequestClose={() => {
        setHelpOpen(false);
      }}
      preventCloseOnClickOutside
      passiveModal
    >
      <p>{textLanguage[language].modalHelp.text}</p>
    </Modal>
  );
}
