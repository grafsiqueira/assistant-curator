const {
  connect,
  createTable,
  insert,
  endConnection,
} = require("../common/database/db2");

function createTables(connStr, tables) {
  const {
    overviewTable,
    classDistributionTable,
    precisionAtKTable,
    classAccuracyTable,
    pairWiseClassErrorsTable,
    accuracyVsCoverageTable,
  } = tables;

  const sqlTables = {
    overviewTable: `CREATE TABLE IF NOT EXISTS CURATOR.${overviewTable} (metric VARCHAR(1000) NOT NULL, value DECIMAL(17,16) NOT NULL,PRIMARY KEY(metric));`,
    classDistributionTable: `CREATE TABLE IF NOT EXISTS CURATOR.${classDistributionTable} (intent VARCHAR(1000) NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(intent));`,
    precisionAtKTable: `CREATE TABLE IF NOT EXISTS CURATOR.${precisionAtKTable} (k INTEGER NOT NULL, precision DECIMAL(17,16) NOT NULL, PRIMARY KEY(k));`,
    classAccuracyTable: `CREATE TABLE IF NOT EXISTS CURATOR.${classAccuracyTable} (class VARCHAR(1000) NOT NULL, count integer NOT NULL, precision DECIMAL(17,16) NOT NULL, recall DECIMAL(17,16) NOT NULL, f1 DECIMAL(17,16) NOT NULL, PRIMARY KEY(class));`,
    pairWiseClassErrorsTable: `CREATE TABLE IF NOT EXISTS CURATOR.${pairWiseClassErrorsTable} (trueClass VARCHAR(1000) NOT NULL, predictedClass VARCHAR(1000) NOT NULL, confidence DECIMAL(17,16) NOT NULL, input VARCHAR(1000) NOT NULL);`,
    accuracyVsCoverageTable: `CREATE TABLE IF NOT EXISTS CURATOR.${accuracyVsCoverageTable} (confidenceThreshold DECIMAL(17,16) NOT NULL, accuracy DECIMAL(17,16) NOT NULL, coverage DECIMAL(17,16) NOT NULL);`,
  };

  return new Promise(async (resolve, reject) => {
    try {
      const conn = await connect(connStr);
      await Promise.all([
        createTable(conn, overviewTable, sqlTables.overviewTable),
        createTable(
          conn,
          classDistributionTable,
          sqlTables.classDistributionTable
        ),
        createTable(conn, precisionAtKTable, sqlTables.precisionAtKTable),
        createTable(conn, classAccuracyTable, sqlTables.classAccuracyTable),
        createTable(
          conn,
          pairWiseClassErrorsTable,
          sqlTables.pairWiseClassErrorsTable
        ),
        createTable(
          conn,
          accuracyVsCoverageTable,
          sqlTables.accuracyVsCoverageTable
        ),
      ]).then(() => {
        endConnection(conn);
        resolve({ result: "success" });
      });
    } catch (error) {
      reject(error);
    }
  });
}

function insertOnDb2(connStr, tables, insertValues) {
  const {
    overviewTable,
    classDistributionTable,
    precisionAtKTable,
    classAccuracyTable,
    pairWiseClassErrorsTable,
    accuracyVsCoverageTable,
  } = tables;

  return new Promise(async (resolve, reject) => {
    try {
      const conn = await connect(connStr);
      await Promise.all([
        insert(conn, overviewTable, insertValues[overviewTable]),
        insert(
          conn,
          classDistributionTable,
          insertValues[classDistributionTable]
        ),
        insert(conn, precisionAtKTable, insertValues[precisionAtKTable]),
        insert(conn, classAccuracyTable, insertValues[classAccuracyTable]),
        insert(
          conn,
          pairWiseClassErrorsTable,
          insertValues[pairWiseClassErrorsTable]
        ),
        insert(
          conn,
          accuracyVsCoverageTable,
          insertValues[accuracyVsCoverageTable]
        ),
      ]).then(() => {
        endConnection(conn);
        resolve("success");
      });
    } catch (error) {
      console.log(error);
      reject("failure");
    }
  });
}

function returnSqlStrings(output, tables) {
  const {
    overviewTable,
    classDistributionTable,
    precisionAtKTable,
    classAccuracyTable,
    pairWiseClassErrorsTable,
    accuracyVsCoverageTable,
  } = tables;
  const newPairwise_class_errors = [];
  const returnObj = {};

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

  returnObj[overviewTable] = agregateSql(output.reports.overview);
  returnObj[classDistributionTable] = agregateSql(
    output.reports.class_distribution
  );
  returnObj[precisionAtKTable] = agregateSql(output.reports.precision_at_k);
  returnObj[classAccuracyTable] = agregateSql(output.reports.class_accuracy);
  returnObj[pairWiseClassErrorsTable] = agregateSql(newPairwise_class_errors);
  returnObj[accuracyVsCoverageTable] = agregateSql(
    output.reports.accuracy_vs_coverage
  );

  return returnObj;
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
