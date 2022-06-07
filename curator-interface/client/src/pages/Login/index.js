import React, { useState, useEffect } from "react";
import "carbon-components/css/carbon-components.min.css";
import Header from "../../components/Header";
import { useGlobalState } from "../../hooks/globalState";
import { TextInput, Button, Loading } from "carbon-components-react";
import {
  getAccounts,
  getResources,
  switchAccount,
} from "../../helpers/apiCalls";
import api from "../../services/api";

import { registerLogin } from "../../helpers/helpers";

import "./style.css";

export default function Login() {
  const {
    setHelpOpen,
    setConfigOpen,
    history,
    setAccount,
    setAccounts,
    setResources,
    logsTable,
    setLogsTable,
    conversationTable,
    setConversationTable,
    callsTable,
    setCallsTable,
    contextTable,
    setContextTable,
    conversationPathTable,
    setConversationPathTable,
    overviewTable,
    setOverviewTable,
    classDistributionTable,
    setClassDistributionTable,
    precisionAtKTable,
    setPrecisionAtKTable,
    classAccuracyTable,
    setClassAccuracyTable,
    pairWiseClassErrorsTable,
    setPairWiseClassErrorsTable,
    accuracyVsCoverageTable,
    setAccuracyVsCoverageTable,
    xsd,
    setXsd,
    driverClassName,
    setDriverClassName,
    cloudantDbName,
    setCloudantDbName,
  } = useGlobalState();
  const [token, setToken] = useState("");
  const [invalid, setInvalid] = useState(false);
  const [loading, setLoading] = useState(false);

  async function handleLogin() {
    setLoading(true);
    try {
      await api.post("/ibmid/login", { passcode: token });
      let res = await getAccounts();
      setAccounts(res);
      if (
        localStorage.getItem("savedAccount") &&
        res.resources.some(
          (account) =>
            JSON.stringify(account) === localStorage.getItem("savedAccount")
        )
      ) {
        console.log("Found account. Picking last one used");
        switchAccount(
          JSON.parse(localStorage.getItem("savedAccount")).metadata.guid
        ).then(() => {
          setAccount(JSON.parse(localStorage.getItem("savedAccount")));
        });
      } else {
        console.log("No saved account. Picking default first");
        setAccount(res.resources[0]);
      }
      setResources(await getResources());
      history.push("/");

      registerLogin(
        `${
          res.resources[0].entity.primary_owner.ibmid
        } - ${new Date().toLocaleDateString("pt-BR", {
          timeZone: "America/Sao_Paulo",
          hour: "numeric",
          minute: "numeric",
          second: "numeric",
        })}`,
        res.resources[0]
      );
    } catch (error) {
      setInvalid(true);
    }
    setLoading(false);
  }

  useEffect(async () => {
    if (localStorage.getItem("xsd")) {
      setXsd(localStorage.getItem("xsd"));
    } else {
      localStorage.setItem("xsd", xsd);
    }
    if (localStorage.getItem("driverClassName")) {
      setDriverClassName(localStorage.getItem("driverClassName"));
    } else {
      localStorage.setItem("driverClassName", driverClassName);
    }
    if (localStorage.getItem("cloudantDbName")) {
      setCloudantDbName(localStorage.getItem("cloudantDbName"));
    } else {
      localStorage.setItem("cloudantDbName", cloudantDbName);
    }
    if (localStorage.getItem("logsTable")) {
      setLogsTable(localStorage.getItem("logsTable"));
    } else {
      localStorage.setItem("logsTable", logsTable);
    }
    if (localStorage.getItem("conversationTable")) {
      setConversationTable(localStorage.getItem("conversationTable"));
    } else {
      localStorage.setItem("conversationTable", conversationTable);
    }
    if (localStorage.getItem("callsTable")) {
      setCallsTable(localStorage.getItem("callsTable"));
    } else {
      localStorage.setItem("callsTable", callsTable);
    }
    if (localStorage.getItem("contextTable")) {
      setContextTable(localStorage.getItem("contextTable"));
    } else {
      localStorage.setItem("contextTable", contextTable);
    }
    if (localStorage.getItem("conversationPathTable")) {
      setConversationPathTable(localStorage.getItem("conversationPathTable"));
    } else {
      localStorage.setItem("conversationPathTable", conversationPathTable);
    }
    if (localStorage.getItem("overviewTable")) {
      setOverviewTable(localStorage.getItem("overviewTable"));
    } else {
      localStorage.setItem("overviewTable", overviewTable);
    }
    if (localStorage.getItem("classDistributionTable")) {
      setClassDistributionTable(localStorage.getItem("classDistributionTable"));
    } else {
      localStorage.setItem("classDistributionTable", classDistributionTable);
    }
    if (localStorage.getItem("precisionAtKTable")) {
      setPrecisionAtKTable(localStorage.getItem("precisionAtKTable"));
    } else {
      localStorage.setItem("precisionAtKTable", precisionAtKTable);
    }
    if (localStorage.getItem("classAccuracyTable")) {
      setClassAccuracyTable(localStorage.getItem("classAccuracyTable"));
    } else {
      localStorage.setItem("classAccuracyTable", classAccuracyTable);
    }
    if (localStorage.getItem("pairWiseClassErrorsTable")) {
      setPairWiseClassErrorsTable(
        localStorage.getItem("pairWiseClassErrorsTable")
      );
    } else {
      localStorage.setItem(
        "pairWiseClassErrorsTable",
        pairWiseClassErrorsTable
      );
    }
    if (localStorage.getItem("accuracyVsCoverageTable")) {
      setAccuracyVsCoverageTable(
        localStorage.getItem("accuracyVsCoverageTable")
      );
    } else {
      localStorage.setItem("accuracyVsCoverageTable", accuracyVsCoverageTable);
    }
  }, []);

  return (
    <div id="login_page">
      <Header modalOpen={setConfigOpen} helpOpen={setHelpOpen} />
      <div id="login_content">
        <div id="login_image">
          <div id="image" />
        </div>

        <div id="login">
          <h2>Login</h2>
          <br></br>
          <TextInput
            size="md"
            type="password"
            labelText="Token"
            placeholder="IBM Cloud one time Token"
            required
            invalid={invalid}
            invalidText="Please, try againg."
            value={token}
            onChange={(event) => {
              setToken(event.target.value);
              setInvalid(false);
            }}
          />
          <div id="buttons">
            <Button
              className={"button"}
              href={`${window.location.protocol}//${window.location.host}/ibmid/passcode`}
              target="_blank"
              kind="secondary"
            >
              Get Token
            </Button>
            <Button onClick={handleLogin} className={"button"}>
              Login
            </Button>
          </div>
        </div>
        {loading && <Loading />}
      </div>
    </div>
  );
}
