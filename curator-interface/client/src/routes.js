import React from "react";
import { Route, Switch, BrowserRouter, Redirect } from "react-router-dom";
import Dashboard from "./pages/Dashboard/index";
import SearchPage from "./pages/Search/index";
import CognosPage from "./pages/Cognos/index";
import Login from "./pages/Login/index";
import GlobalStateProvider from "./hooks/globalState";

export default function Routes() {
  return (
    <BrowserRouter>
      <GlobalStateProvider>
        <Switch>
          <Route exact path="/" component={Dashboard} />
          <Route exact path="/dashboard" component={Dashboard} />
          <Route exact path="/login" component={Login} />
          <Route exact path="/search" component={SearchPage} />
          <Route exact path="/cognos" component={CognosPage} />
        </Switch>
      </GlobalStateProvider>
    </BrowserRouter>
  );
}
