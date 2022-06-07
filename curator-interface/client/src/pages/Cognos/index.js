import React, { useEffect, useState } from "react";
import { useGlobalState } from "../../hooks/globalState";
import Header from "../../components/Header";

import CognosApi from "../../helpers/cognos";
import {
  getCognosSession,
  getFromCloudant,
  initializeSources,
} from "../../helpers/helpers";

import "./style.css";

import CognosConfigModal from "../../components/CognosConfigModal";

export default function CognosPage() {
  const {
    account,
    history,
    language,
    setLanguage,
    setCognosConfigOpen,
    setCognosHelpOpen,
    setCognosClient,
    cognosSession,
    setCognosSession,
    cognosUsername,
    cognosPassword,
    cognosDashboard,
    setCognosDashboard,
    setSuccessOpen,
    setUnsavedChanges,
    setStandardDashboardModal,
    cloudantApi,
    cloudantUrl,
    cloudantDbName,
    defaultDashboardName,
    xsd,
    jdbcUrl,
    driverClassName,
    schema,
    db2Username,
    db2Password,
    logsTable,
    conversationTable,
    callsTable,
    contextTable,
    conversationPathTable,
    overviewTable,
    classDistributionTable,
    precisionAtKTable,
    classAccuracyTable,
    pairWiseClassErrorsTable,
    accuracyVsCoverageTable,
  } = useGlobalState();

  const [renderSave, setRenderSave] = useState(false);

  useEffect(() => {
    if (account.entity.name === "") history.push("/login");
  }, []);

  async function loadDashboard(cognosApi, dashboard, setUnsavedChanges) {
    return new Promise((resolve, reject) => {
      cognosApi.initialize().then(() => {
        cognosApi.dashboard
          .openDashboard({
            dashboardSpec: dashboard,
          })
          .then(async (dashboardAPI) => {
            dashboardAPI.setMode(dashboardAPI.MODES.EDIT);
            setCognosDashboard(await dashboardAPI.getSpec());

            dashboardAPI.on(dashboardAPI.EVENTS.DIRTY, async () => {
              const dashSpec = await dashboardAPI.getSpec();
              setUnsavedChanges(true);
              setCognosDashboard(dashSpec);
            });

            setRenderSave(true);
            resolve("Ok");
          });
      });
    });
  }

  async function initializeDashboard(cognosApi) {
    cognosApi.initialize().then(async () => {
      cognosApi.dashboard
        .openDashboard({
          dashboardSpec: await initializeSources(
            xsd,
            jdbcUrl,
            driverClassName,
            schema,
            db2Username,
            db2Password,
            logsTable,
            conversationTable,
            callsTable,
            contextTable,
            conversationPathTable,
            overviewTable,
            classDistributionTable,
            precisionAtKTable,
            classAccuracyTable,
            pairWiseClassErrorsTable,
            accuracyVsCoverageTable
          ),
        })
        .then(async (dashboardAPI) => {
          dashboardAPI.setMode(dashboardAPI.MODES.EDIT);
          setCognosDashboard(await dashboardAPI.getSpec());

          dashboardAPI.on(dashboardAPI.EVENTS.DIRTY, async () => {
            const dashSpec = await dashboardAPI.getSpec();
            setUnsavedChanges(true);
            setCognosDashboard(dashSpec);
          });

          setRenderSave(true);
        });
    });
  }

  useEffect(async () => {
    const storedLanguage = localStorage.getItem("language");
    if (storedLanguage) setLanguage(storedLanguage);

    setSuccessOpen(false);
    setStandardDashboardModal(false);

    if (
      cognosUsername &&
      cognosUsername !== "null" &&
      cognosPassword &&
      cognosPassword !== "null"
    ) {
      const session = await getCognosSession(cognosUsername, cognosPassword);
      if (session.data.Error) {
        setCognosSession(null);
      } else {
        setCognosSession(session.data);
      }
    } else {
      setCognosConfigOpen(true);
    }
  }, []);

  useEffect(async () => {
    if (cognosSession && typeof cognosSession === "string") {
      const cognosApi = new CognosApi({
        cognosRootURL:
          "https://us-south.dynamic-dashboard-embedded.cloud.ibm.com/daas/",
        node: document.getElementById("cognosDiv"),
        sessionCode: cognosSession,
        language: language,
      });
      setCognosClient(cognosApi);

      if (cognosDashboard) {
        await loadDashboard(cognosApi, cognosDashboard, setUnsavedChanges);
      } else {
        const storagedDashboard = await getFromCloudant(
          defaultDashboardName,
          cloudantApi,
          cloudantUrl,
          cloudantDbName,
          setStandardDashboardModal
        );
        setCognosConfigOpen(false);
        if (!storagedDashboard.Error) {
          await loadDashboard(cognosApi, storagedDashboard, setUnsavedChanges);
        } else {
          await initializeDashboard(cognosApi);
        }
      }
    } else if (!cognosSession) {
      setRenderSave(false);
      setCognosClient(null);
    }
  }, [cognosSession]);

  return (
    <div id="content">
      <Header
        modalOpen={setCognosConfigOpen}
        helpOpen={setCognosHelpOpen}
        renderButton={renderSave}
      />
      <CognosConfigModal />
      <div id="cognosDiv"></div>
    </div>
  );
}
