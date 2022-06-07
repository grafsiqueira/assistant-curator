const cosSdk = require("ibm-cos-sdk");

function uploadItem(cosConfig, itemName, data) {
  return new Promise(async (resolve, reject) => {
    try {
      const cos = new cosSdk.S3(cosConfig);
      cos.putObject(
        {
          Bucket: cosConfig.bucketName,
          Key: itemName,
          Body: data,
        },
        function (err) {
          if (err) throw err;
          return resolve(console.log("Inserted on COS"));
        }
      );
    } catch (err) {
      reject(err);
    }
  });
}

module.exports = { uploadItem };
