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

import { registerLogin, defaultValues } from "../../helpers/helpers";

import "./style.css";

export default function Login() {
  const {
    setHelpOpen,
    setConfigOpen,
    history,
    setAccount,
    setAccounts,
    setResources,
    credentialsAndDefaults,
    setCredentialsAndDefaults,
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
        await switchAccount(
          JSON.parse(localStorage.getItem("savedAccount")).metadata.guid
        ).then(() => {
          setAccount(JSON.parse(localStorage.getItem("savedAccount")));
        });
      } else {
        console.log("No saved account. Picking default first");
        setAccount(res.resources[0]);
      }
      setResources(await getResources());
      history.push("/dashboard");

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

  useEffect(() => {
    let temp = { ...credentialsAndDefaults };
    if (credentialsAndDefaults === null) {
      temp = defaultValues;
    } else {
      Object.entries(credentialsAndDefaults.defaults).map(([key, value]) => {
        if (value === null) {
          temp.defaults[key] = defaultValues.defaults[key];
        }
      });
    }
    setCredentialsAndDefaults(temp);
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
