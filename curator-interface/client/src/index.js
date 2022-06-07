import React from 'react';
import ReactDOM from 'react-dom';
import './global.css';
import Routes from './routes';
import GlobalStateProvider from './hooks/globalState';

ReactDOM.render(
  <React.StrictMode>
    <GlobalStateProvider>
      <Routes />
    </GlobalStateProvider>
  </React.StrictMode>,
  document.getElementById('root')
);
