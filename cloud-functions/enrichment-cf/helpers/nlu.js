const NaturalLanguageUnderstandingV1 = require("ibm-watson/natural-language-understanding/v1");
const { IamAuthenticator } = require("ibm-watson/auth");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function queryNlu(objects, language, version, apikey, serviceUrl) {
  const naturalLanguageUnderstanding = new NaturalLanguageUnderstandingV1({
    version: version,
    authenticator: new IamAuthenticator({
      apikey: apikey,
    }),
    serviceUrl: serviceUrl,
  });

  for (let object of objects) {
    if (object.clientMessage) {
      const analyzeParams = {
        text: object.clientMessage,
        language: language,
        features: {
          sentiment: {
            document: true,
          },
        },
      };
      naturalLanguageUnderstanding.analyze(analyzeParams).then((response) => {
        object.sentiment = response.result.sentiment.document.score;
      });
    }
  }
  await sleep(2001);
}

module.exports = {
  queryNlu,
};
