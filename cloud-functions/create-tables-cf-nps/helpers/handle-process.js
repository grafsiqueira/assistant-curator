const {
  connect,
  createPrimaryTable,
  createSecondaryTable,
  createTertiaryTable,
  createQuaternaryTable,
  createQuinaryTable,
  endConnection,
} = require("../database/db2");

function createTables(
  connStr,
  primaryTableName,
  secondaryTableName,
  tertiaryTableName,
  quaternaryTableName,
  quinaryTableName
) {
  return new Promise(async (resolve, reject) => {
    try {
      const conn = await connect(connStr);
      await Promise.all([
        createPrimaryTable(conn, primaryTableName),
        createSecondaryTable(conn, secondaryTableName),
        createTertiaryTable(conn, tertiaryTableName),
        createQuaternaryTable(conn, quaternaryTableName),
        createQuinaryTable(conn, quinaryTableName),
      ]).then(endConnection(conn));

      resolve({ result: "success" });
    } catch (error) {
      reject(error);
    }
  });
}

module.exports = {
  createTables,
};
