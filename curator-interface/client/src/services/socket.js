import socketIOClient from 'socket.io-client';

const ENDPOINT = `${window.location.protocol}//${window.location.host}`;
// const ENDPOINT = `http://127.0.0.1:3000`;

export const socket = socketIOClient(ENDPOINT);
