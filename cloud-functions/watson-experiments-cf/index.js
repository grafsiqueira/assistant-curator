const { runExperiment } = require("./helpers/experiment");
const {
  createTables,
  insertOnDb2,
  returnSqlStrings,
} = require("./helpers/db2");

function main(params) {
  const { assistantCreds, db2Creds } = params;
  return new Promise(async (resolve, reject) => {
    try {
      await createTables(db2Creds.connStr, db2Creds.tables);
      const experimentResults = await runExperiment(assistantCreds);
      await insertOnDb2(
        db2Creds.connStr,
        db2Creds.tables,
        returnSqlStrings(experimentResults, db2Creds.tables)
      );
      resolve({ body: experimentResults });
    } catch (error) {
      console.log(error);
      resolve({ body: error });
    }
  });
}

//Do not forget to comment the following lines when zipping to deploy as cloud function over IBM Cloud
// const invocation = require("./invocation.json");
// const fs = require("fs");
// async function test() {
//   fs.writeFileSync(
//     "output.json",
//     JSON.stringify(await main(invocation), null, 2),
//     () => {}
//   );
// }
// test();

module.exports = {
  main,
};
