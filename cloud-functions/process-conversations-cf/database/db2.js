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
    console.log("Connection closed.");
  });
}

function checkIfNew(conn, table, sqlString) {
  return new Promise((resolve, reject) => {
    var sql = `SELECT IDUSER FROM CURATOR.${table} WHERE ${sqlString};`;

    try {
      conn.query(sql, (err, data) => {
        if (err) throw err;
        return resolve(data);
      });
    } catch (error) {
      reject(error);
    }
  });
}

module.exports = {
  connect,
  checkIfNew,
  endConnection,
};
