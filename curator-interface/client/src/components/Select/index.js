import React from "react";
import { useGlobalState } from "../../hooks/globalState";
import { Select, SelectItem } from "carbon-components-react";

import textLanguage from "../../helpers/languagesConfig";

export default function CustomSelect({ index, setSelection, field }) {
  const { resources, language } = useGlobalState();

  return (
    <Select
      id={`select-${index}`}
      invalidText="A valid value is required"
      labelText={textLanguage[language].cognosModal[`inputLabel${index}`]}
      onChange={(e) => {
        if (e.target.value === "placeholder")
          alert(textLanguage[language].cognosModal.alert);
        else setSelection(JSON.parse(e.target.value));
      }}
    >
      <SelectItem
        text={textLanguage[language].cognosModal.selectAnOption}
        value="placeholder"
      />
      {resources[field].body.resources.map((resource) => (
        <SelectItem text={resource.name} value={JSON.stringify(resource)} />
      ))}
    </Select>
  );
}
