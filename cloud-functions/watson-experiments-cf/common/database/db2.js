process.env.DB2CODEPAGE = process.env.DB2CODEPAGE || 1208;
const ibmdb = require("ibm_db");

function connect(connStr) {
  return new Promise((resolve, reject) => {
    try {
      ibmdb.open(connStr, function (err, conn) {
        if (err) reject(err);
        resolve(conn);
      });
    } catch (error) {
      console.log(error);
    }
  });
}

function endConnection(conn) {
  conn.close(function () {
    console.log("Connection closed.");
  });
}

function createTable(conn, name, sql) {
  return new Promise((resolve, reject) => {
    try {
      conn.query(sql, async (err) => {
        if (err) {
          //if tables already exist, first delete them, to refresh Db2 with new data
          if (err.sqlcode === 4136 || err.sqlcode === -601) {
            await conn.query(`DROP TABLE CURATOR.${name};`, () => {
              conn.query(sql, (err) => {
                if (err) throw err;
                resolve("Table refreshed! Starting from 0.");
              });
            });
          } else {
            throw err;
          }
        } else {
          resolve("Table created!");
        }
      });
    } catch (error) {
      reject(error);
    }
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
  createTable,
  insert,
  endConnection,
};
