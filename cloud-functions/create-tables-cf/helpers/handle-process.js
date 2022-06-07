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

      await createPrimaryTable(conn, primaryTableName);
      await createSecondaryTable(conn, secondaryTableName);
      await createTertiaryTable(conn, tertiaryTableName);
      await createQuaternaryTable(conn, quaternaryTableName);
      await createQuinaryTable(conn, quinaryTableName);

      endConnection(conn);

      resolve({ result: "success" });
    } catch (error) {
      reject(error);
    }
  });
}

module.exports = {
  createTables,
};
