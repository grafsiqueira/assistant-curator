// import dependencies and initialize express
const express = require("express");
const path = require("path");
const http = require("http");
const cors = require("cors");
const cookieParser = require("cookie-parser");

const app = express();
const server = http.createServer(app);

const { default: IBMidService } = require("ibmid-login");
const ibmidLogin = new IBMidService();

global.ibmidLogin = ibmidLogin;

const routes = require("./routes/route");

// enable parsing of http request body
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb" }));
app.use(cookieParser());
app.use(cors());
app.use(express.static(path.join(__dirname, "../build")));

// routes and api calls
app.use("/ibmid", ibmidLogin.expressAdapter);
app.use("/", routes);

app.use(express.static(path.join(__dirname, "../build")));

app.use("", (req, res) => {
  res.status(200).sendFile(path.join(__dirname, "../build", "index.html"));
});

// start node server
const port = process.env.PORT || 5000;
server.listen(port, () => {
  console.log(`App UI available http://localhost:${port}`);
});

module.exports = app;
