'use strict';

module.exports.handler = async (event) => {
  const records = event.Records;

  for (const record of records) {
      const data = record.kinesis.data;
      const decodedData = new Buffer.from(data, 'base64').toString('utf-8');

      const dataDict = JSON.parse(decodedData);
      console.log(JSON.stringify(dataDict));

      // Process the data as needed
      // ...
  }

  return {
      statusCode: 200,
      body: 'Success',
  };
};
