import { useEffect, useState } from "react";
import { useGlobalState } from "../../hooks/globalState";
import { Modal, Select, SelectItem } from "carbon-components-react";
import { switchAccount } from "../../helpers/apiCalls";
import textLanguage from "../../helpers/languagesConfig";

import "./style.css";

export default function AccountSelectionModal({ modalOpen }) {
  const {
    setLoading,
    accountModalOpen,
    setAccountModalOpen,
    setWarningOpen,
    setSuccessOpen,
    accounts,
    setAccount,
    setAccountLoading,
    language,
  } = useGlobalState();

  async function handleSubmit() {
    setAccountLoading(true);
    setWarningOpen(false);
    setSuccessOpen(false);
    setAccountModalOpen(false);
    setLoading(true);
    modalOpen(true);
  }

  return (
    <Modal
      open={accountModalOpen}
      modalHeading={textLanguage[language].accountSelectionModal.heading}
      modalLabel={textLanguage[language].accountSelectionModal.label}
      primaryButtonText={
        textLanguage[language].accountSelectionModal.primaryText
      }
      secondaryButtonText={
        textLanguage[language].accountSelectionModal.secondaryText
      }
      onRequestClose={() => {
        setAccountModalOpen(false);
      }}
      onRequestSubmit={handleSubmit}
      preventCloseOnClickOutside
    >
      <Select
        id="select-1"
        invalidText="A valid value is required"
        labelText={textLanguage[language].accountSelectionModal.inputLabel1}
        onChange={(e) => {
          switchAccount(JSON.parse(e.target.value).metadata.guid).then(() => {
            localStorage.setItem("savedAccount", e.target.value);
            setAccount(JSON.parse(e.target.value));
          });
        }}
      >
        {accounts &&
          accounts.resources.map((resource) => (
            <SelectItem
              text={resource.entity.name}
              value={JSON.stringify(resource)}
            />
          ))}
      </Select>
    </Modal>
  );
}
