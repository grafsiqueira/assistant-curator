import React from "react";
import { useGlobalState } from "../../hooks/globalState";
import { TextInput } from "carbon-components-react";

import textLanguage from "../../helpers/languagesConfig";

export default function Input({ index, field, upperCase }) {
  const { language, credentialsAndDefaults, setCredentialsAndDefaults } =
    useGlobalState();

  function handleChange(e) {
    let temp = { ...credentialsAndDefaults };
    temp.defaults[field] = upperCase
      ? e.target.value.toUpperCase()
      : e.target.value;
    setCredentialsAndDefaults(temp);
  }
  return (
    <TextInput
      data-modal-primary-focus
      id={`text-input-${index}`}
      labelText={textLanguage[language].cognosModal[`inputLabel${index}`]}
      placeholder={credentialsAndDefaults.defaults[field]}
      style={{ marginBottom: "1rem" }}
      onChange={handleChange}
    />
  );
}
