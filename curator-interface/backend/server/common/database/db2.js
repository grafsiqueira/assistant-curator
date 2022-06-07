process.env.DB2CODEPAGE = process.env.DB2CODEPAGE || 1208;
var ibmdb = require("ibm_db");

function connect(connStr) {
  return new Promise((resolve, reject) => {
    ibmdb.open(connStr, function (err, conn) {
      if (err) reject(err);
      resolve(conn);
    });
  });
}

function endConnection(conn) {
  return new Promise((resolve, reject) => {
    try {
      conn.close(function () {
        resolve("Db2 connection closed.");
      });
    } catch (error) {
      reject(error);
    }
  });
}

function update(conn, table, newValue, clause) {
  return new Promise((resolve, reject) => {
    try {
      const sql = `update CURATOR.${table} set ${newValue} where ${clause}`;
      conn.querySync(sql);
      resolve("Updated rows");
    } catch (error) {
      reject(error);
    }
  });
}

function select(conn, table, clause) {
  return new Promise(async (resolve, reject) => {
    var sql = `SELECT * FROM CURATOR.${table} WHERE ${clause};`;
    try {
      const rows = conn.querySync(sql);
      resolve(Array.from(rows));
    } catch (error) {
      reject(error);
    }
  });
}

module.exports = {
  connect,
  endConnection,
  update,
  select,
};
