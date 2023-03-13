"use strict";

module.exports.handler = async (event) => {
  const records = event.Records;
  const FAILED_MESSAGE_NUM = 5;

  for (const [i, record] of records) {
    const data = record.kinesis.data;

    if (i === FAILED_MESSAGE_NUM) {
      console.log("Error! ", sequenceNumber);
      throw new Error("kaboom");
    }
    const decodedData = new Buffer.from(data, "base64").toString("utf-8");
    console.log(decodedData);
  }
  return {
    statusCode: 200,
    body: "Success",
  };
};
