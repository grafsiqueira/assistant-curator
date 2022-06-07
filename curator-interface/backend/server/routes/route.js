// import dependencies and initialize the express router
const express = require("express");
const {
  getLogs,
  updateScore,
  cognosSession,
  insertOnCloudantController,
  getFromCloudantController,
  initializeDashboardController,
  registerLoginController,
} = require("../useCases/management/controller");

const { getResources } = require("../useCases/management/resources");

const router = express.Router();

// define routes
router.post("/getLogs", getLogs);
router.post("/updateScore", updateScore);
router.post("/cognosSession", cognosSession);
router.post("/insertDashboard", insertOnCloudantController);
router.post("/getDashboard", getFromCloudantController);
router.post("/initializeDashboard", initializeDashboardController);
router.post("/registerLogin", registerLoginController);
router.get("/resources", getResources);

module.exports = router;
