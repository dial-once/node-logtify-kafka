# logtify-kafka
[![CircleCI](https://circleci.com/gh/dial-once/node-logtify-kafka.svg?style=svg)](https://circleci.com/gh/dial-once/node-logtify-kafka)

Kafka plugin for logtify logger

## Installation
```
npm i -S logtify-kafka
```

## Usage
Used with [logtify](https://github.com/dial-once/node-logtify) module.

```js
require('logtify-kafka')({ KAFKA_HOST: 'kafka-broker.io:9092', KAFKA_TOPIC: 'example' });
const { stream, logger } = require('logtify')();
logger.log('error', new Error('Test error'));
logger.info('Hello world!');
```

The subscriber will send the given data to Kafka broker if:
* ``message.level >= ('MIN_LOG_LEVEL_KAFKA' || 'MIN_LOG_LEVEL')``
* ``process.env.KAFKA_LOGGING === 'true' || settings.KAFKA_LOGGING === true``
* if not given, process.env.KAFKA_LOGGING is considered `true` by default


**Settings**:
Module can be configured by both env variables or config object. However, env variables have a higher priority.
```js
{
  KAFKA_HOST: 'kafka-broker:9092',
  KAFKA_TOPIC: 'example', // the plugin will attempt to create this topic on start
  KAFKA_LOGGING: true|false, // true by default
  KAFKA_CONNECT_TIMEOUT: 5000,
  KAFKA_REQUEST_TIMEOUT: 10000,
  MIN_LOG_LEVEL_KAFKA: 'silly|verbose|info|warn|error',
  LOG_TIMESTAMP: 'true',
  LOG_ENVIRONMENT: 'true',
  LOG_LEVEL: 'true',
  LOG_REQID: 'true', // only included when provided with metadata
  LOG_CALLER_PREFIX: 'true' // additional prefix with info about caller module/project/function
}
```
