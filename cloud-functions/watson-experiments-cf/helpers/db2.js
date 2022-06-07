const {
  connect,
  createTable,
  insert,
  endConnection,
} = require("../common/database/db2");

function createTables(connStr) {
  const sqlTables = [
    [
      "overview",
      "CREATE TABLE IF NOT EXISTS CURATOR.overview (metric VARCHAR(1000) NOT NULL, value DECIMAL(17,16) NOT NULL,PRIMARY KEY(metric));",
    ],
    [
      "classDistribution",
      "CREATE TABLE IF NOT EXISTS CURATOR.classDistribution (intent VARCHAR(1000) NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(intent));",
    ],
    [
      "precisionAtK",
      "CREATE TABLE IF NOT EXISTS CURATOR.precisionAtK (k INTEGER NOT NULL, precision DECIMAL(17,16) NOT NULL, PRIMARY KEY(k));",
    ],
    [
      "classAccuracy",
      "CREATE TABLE IF NOT EXISTS CURATOR.classAccuracy (class VARCHAR(1000) NOT NULL, count integer NOT NULL, precision DECIMAL(17,16) NOT NULL, recall DECIMAL(17,16) NOT NULL, f1 DECIMAL(17,16) NOT NULL, PRIMARY KEY(class));",
    ],
    [
      "pairWiseClassErrors",
      "CREATE TABLE IF NOT EXISTS CURATOR.pairWiseClassErrors (trueClass VARCHAR(1000) NOT NULL, predictedClass VARCHAR(1000) NOT NULL, confidence DECIMAL(17,16) NOT NULL, input VARCHAR(1000) NOT NULL);",
    ],
    [
      "accuracyVsCoverage",
      "CREATE TABLE IF NOT EXISTS CURATOR.accuracyVsCoverage (confidenceThreshold DECIMAL(17,16) NOT NULL, accuracy DECIMAL(17,16) NOT NULL, coverage DECIMAL(17,16) NOT NULL);",
    ],
  ];

  return new Promise(async (resolve, reject) => {
    try {
      const conn = await connect(connStr);
      for (let table of sqlTables) {
        console.log(await createTable(conn, table[0], table[1]));
      }
      endConnection(conn);
      resolve({ result: "success" });
    } catch (error) {
      reject(error);
    }
  });
}

function insertOnDb2(connStr, insertValues) {
  const sqlTables = [
    "overview",
    "classDistribution",
    "precisionAtK",
    "classAccuracy",
    "pairWiseClassErrors",
    "accuracyVsCoverage",
  ];

  return new Promise(async (resolve, reject) => {
    try {
      const conn = await connect(connStr);
      for (let table of sqlTables) {
        console.log(await insert(conn, table, insertValues[table]));
      }
      endConnection(conn);
      resolve("success");
    } catch (error) {
      console.log(error);
      reject("failure");
    }
  });
}

function returnSqlStrings(output) {
  const newPairwise_class_errors = [];

  for (let obj of output.reports.pairwise_class_errors) {
    for (let error of obj.errors) {
      newPairwise_class_errors.push({
        true_class: obj.true_class,
        predicted_class: error.predicted_class,
        confidence: error.confidence,
        input: error.input.text ? error.input.text : "",
      });
    }
  }
  return {
    overview: agregateSql(output.reports.overview),
    classDistribution: agregateSql(output.reports.class_distribution),
    precisionAtK: agregateSql(output.reports.precision_at_k),
    classAccuracy: agregateSql(output.reports.class_accuracy),
    pairWiseClassErrors: agregateSql(newPairwise_class_errors),
    accuracyVsCoverage: agregateSql(output.reports.accuracy_vs_coverage),
  };
}

function agregateSql(objects) {
  let sqlStrings = [];
  for (let object of objects) {
    sqlStrings.push(createSqlString(object));
  }
  return sqlStrings;
}

function createSqlString(params) {
  let values = [];
  Object.entries(params).map(([key, value]) => {
    if (key.includes("Time"))
      values.push(
        `(TIMESTAMP(CAST('${value.date}' AS VARCHAR(10)),'${value.time}'))`
      );
    else values.push(`'${value}'`);
  });
  return `(${values.join(",")})`;
}

module.exports = {
  createTables,
  insertOnDb2,
  returnSqlStrings,
};
