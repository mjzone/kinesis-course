'use strict';

module.exports.handler = async (event) => {
  const records = event.Records;

  for (const record of records) {
      const partitionKey = record.kinesis.partitionKey;
      const data = record.kinesis.data;
      const decodedData = new Buffer.from(data, 'base64').toString('utf-8');
      // const parsedData = JSON.parse(decodedData);
      console.log("PK:" + partitionKey , decodedData);
  }
  return {
      statusCode: 200,
      body: 'Success',
  };
};
