import { useGlobalState } from "../../hooks/globalState";
import { Modal } from "carbon-components-react";
import textLanguage from "../../helpers/languagesConfig";
import "./style.css";

export default function CognosHelpModal() {
  const { language, cognosHelpOpen, setCognosHelpOpen } = useGlobalState();

  return (
    <Modal
      open={cognosHelpOpen}
      modalHeading={textLanguage[language].cognosModalHelp.heading}
      onRequestClose={() => {
        setCognosHelpOpen(false);
      }}
      preventCloseOnClickOutside
      passiveModal
    >
      <p>{textLanguage[language].cognosModalHelp.text}</p>
    </Modal>
  );
}
