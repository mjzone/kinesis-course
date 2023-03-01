"use strict";
const AWS = require("aws-sdk");
AWS.config.update({ region: process.env.AWS_REGION });
const docClient = new AWS.DynamoDB.DocumentClient();
const TELEMETRY_TABLE_NAME = process.env.TELEMETRY_TABLE_NAME;

function isEmpty(obj) {
  return Object.keys(obj).length === 0;
}

exports.handler = async (event) => {
  // Save aggregation result in the final invocation
  if (event.isFinalInvokeForWindow) {
    console.log("Final: ", event);

    const siteIds = Object.keys(event.state);
    let params = { RequestItems: {} };
    params.RequestItems[TELEMETRY_TABLE_NAME] = [];

    siteIds.forEach((siteId, index) => {
      let site = event.state[siteId];
      let avg_temp = site.temp / site.count;
      let avg_pressure = site.pressure / site.count;
      let avg_wind = site.wind / site.count;

      params.RequestItems[TELEMETRY_TABLE_NAME].push({
        PutRequest: {
          Item: {
            siteId,
            timestamp: new Date().toJSON(),
            avgTemp: +parseFloat(avg_temp).toFixed(2),
            avgPressure: +parseFloat(avg_pressure).toFixed(2),
            avgWind: +parseFloat(avg_wind).toFixed(2),
            windowEnd: event.window.end,
            windowStart: event.window.start,
            shardId: event.shardId,
          },
        },
      });
    });

    await docClient.batchWrite(params).promise();
  }

  // Create the state object on first invocation or use state passed in
  let state = event.state;

  if (isEmpty(state)) {
    state = {};
  }
  console.log("Existing: ", state);

  // Process records with custom aggregation logic
  event.Records.map((record) => {
    const partitionKey = record.kinesis.partitionKey;
    const data = record.kinesis.data;
    const payload = new Buffer.from(data, "base64").toString("utf-8");
    const item = JSON.parse(payload);

    if (!state[partitionKey]) {
      state[partitionKey] = { temp: 0, pressure: 0, wind: 0, count: 0 };
    }

    console.log("=== Count 0 ====");
    console.log(state[partitionKey]);

    state[partitionKey].temp += item.temperature;
    state[partitionKey].pressure += item.pressure;
    state[partitionKey].wind += item.wind;
    state[partitionKey].count += 1;

    console.log("=== Count 1 ====");
    console.log(state[partitionKey]);
  });

  // Return the state for the next invocation
  console.log("Returning state: ", state);
  return { state: state };
};
