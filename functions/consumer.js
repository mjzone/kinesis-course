"use strict";

module.exports.handler = async (event) => {
  const records = event.Records;

  records.forEach((record, i) => {
    const data = record.kinesis.data;

    const decodedData = new Buffer.from(data, "base64").toString("utf-8");
    console.log(decodedData);
    
    const parsedData = JSON.parse(decodedData);

    if (parsedData.error) {
      console.log("Error! ", record.kinesis.sequenceNumber);
      throw new Error("kaboom");
    }
  });

  return {
    statusCode: 200,
    body: "Success",
  };
};
