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
  conn.close(function () {
    console.log("Db2 connection closed.");
  });
}

function insert(conn, table, sqlString) {
  return new Promise((resolve, reject) => {
    if (sqlString.length === 0) return resolve("Nothing to insert!");
    var sql = `INSERT INTO CURATOR.${table} VALUES ${sqlString};`;

    try {
      conn.query(sql, function (err) {
        if (err) throw err;
        else {
          return resolve("Inserted!");
        }
      });
    } catch (error) {
      reject(error);
    }
  });
}

module.exports = {
  connect,
  insert,
  endConnection,
};
