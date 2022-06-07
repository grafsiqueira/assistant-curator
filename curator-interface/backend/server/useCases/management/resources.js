const ibmidLogin = global.ibmidLogin;

async function getResources(req, res) {
  const token = req.ibmid ? req.ibmid.token : req.cookies.token;

  const cognos = await ibmidLogin.listResources({
    token,
    resourceType: "dynamic-dashboard-embedded",
  });
  const cloudant = await ibmidLogin.listResources({
    token,
    resourceType: "cloudantnosqldb",
  });
  const db2 = await ibmidLogin.listResources({
    token,
    resourceType: "dashdb-for-transactions",
  });
  res.send({ cognos, cloudant, db2 });
}

module.exports = { getResources };
