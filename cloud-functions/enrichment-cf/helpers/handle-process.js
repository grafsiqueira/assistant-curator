const { queryNlu } = require("./nlu");

async function findSentimentScore(
  objects,
  language,
  version,
  apikey,
  serviceUrl
) {
  for (let beginning = 0; beginning < objects.length; beginning += 150) {
    let slicedArray = objects.slice(beginning, beginning + 150);
    console.log(slicedArray.length);
    await queryNlu(slicedArray, language, version, apikey, serviceUrl);
  }
}

module.exports = {
  findSentimentScore,
};
