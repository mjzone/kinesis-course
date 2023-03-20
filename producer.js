const AWS = require("aws-sdk");

const kinesis = new AWS.Kinesis({
  region: "us-east-1",
});

const streamName = "KP1-dev-KP1DataStream-zH47e6qZcRvQ";
let i = 0;

function generateTelemetry(siteId) {
  setInterval(() => {
    const temperature = 20 + Math.random() * 10;
    const wind = Math.random() * 10;
    const pressure = 980 + Math.random() * 40;
    const telemetry = { temperature, wind, pressure };

    if (i === 5) telemetry.error = true;
    i++;

    const params = {
      Data: JSON.stringify(telemetry),
      PartitionKey: siteId,
      StreamName: streamName,
    };

    kinesis.putRecord(params, (err, data) => {
      if (err) {
        console.error(err, err.stack);
      } else {
        console.log(data);
      }
    });
  }, 500);
}

generateTelemetry("S01");
