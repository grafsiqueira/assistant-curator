var ibmdb = require("ibm_db");

function connect(connStr) {
  return new Promise((resolve, reject) => {
    ibmdb.open(connStr, function (err, conn) {
      if (err) {
        reject(err);
      }
      resolve(conn);
    });
  });
}

function endConnection(conn) {
  conn.close(function () {
    console.log("Connection closed");
  });
}

function lastRecord(conn, table) {
  return new Promise((resolve, reject) => {
    var sql = `select max(assistantTimestamp) from CURATOR.${table};`;
    try {
      conn.query(sql, function (err, data) {
        if (err) console.log(`lastRecord error: ${err}`);
        else {
          if (data) {
            return resolve(data[0]);
          } else {
            return resolve(null);
          }
        }
      });
    } catch (error) {
      reject(error);
    }
  });
}

module.exports = {
  connect,
  lastRecord,
  endConnection,
};
