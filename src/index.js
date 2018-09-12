const { Producer, KafkaClient } = require('kafka-node');
const logtify = require('logtify');

const { streamBuffer } = logtify;
const { stream, adapters } = logtify();

class Kafka extends stream.Subscriber {
  constructor(configs) {
    super();
    this.ready = false;
    this.settings = configs || {};
    this.kafkaClient = undefined;
    this.kafkaProducer = undefined;
    if (this.settings.KAFKA_URL && this.settings.KAFKA_TOPIC) {
      this.initialize();
    } else {
      console.warn('Kafka logtify module is not active due to a missing KAFKA_URL and/or KAFKA_TOPIC');
    }

    this.cleanup = this.cleanup.bind(this);

    process.once('exit', this.cleanup);
    process.once('SIGINT', this.cleanup);
    process.once('SIGTERM', this.cleanup);
    process.once('uncaughtException', this.cleanup);

    this.name = 'KAFKA';
  }

  initialize() {
    this.kafkaClient = new KafkaClient({
      kafkaHost: this.settings.KAFKA_URL,
      connectTimeout: this.settings.KAFKA_CONNECT_TIMEOUT || 5000,
      requestTimeout: this.settings.KAFKA_REQUEST_TIMEOUT || 10000,
      autoConnect: true
    });
    this.kafkaProducer = new Producer(this.kafkaClient);
    this.kafkaProducer.once('ready', () => {
      this.ready = true;
      this.kafkaProducer.createTopics([this.settings.KAFKA_TOPIC], false, () => {});
    });
    this.kafkaProducer.on('error', (e) => {
      const defaultLogger = adapters.get('logger');
      if (defaultLogger && defaultLogger.winston) {
        defaultLogger.winston.error(e);
      }
    });
  }

  isEnabled() {
    const result = ['true', 'false'].includes(process.env.KAFKA_LOGGING)
      ? process.env.KAFKA_LOGGING === 'true'
      : this.settings.KAFKA_LOGGING;
    // enabled by default
    return [null, undefined].includes(result) ? true : result;
  }

  cleanup() {
    if (this.kafkaClient) {
      this.kafkaClient.close();
      this.kafkaProducer.removeAllListeners('error');
    }
  }

  handle(message) {
    if (this.ready && this.isEnabled() && message) {
      const content = message.payload;
      const messageLevel = this.logLevels.has(content.level) ? content.level : this.logLevels.get('default');
      const minLogLevel = this.getMinLogLevel(this.settings, this.name);
      if (this.logLevels.get(messageLevel) >= this.logLevels.get(minLogLevel)) {
        const metadata = message.jsonifyMetadata();
        this.kafkaProducer.send([{
          topic: this.settings.KAFKA_TOPIC,
          messages: {
            level: messageLevel,
            text: content.text,
            prefix: message.getPrefix(this.settings),
            metadata
          }
        }]);
      }
    }
  }
}

module.exports = (config) => {
  const configs = Object.assign({
    KAFKA_URL: process.env.KAFKA_URL,
    KAFKA_TOPIC: process.env.KAFKA_TOPIC
  }, config);
  const streamLinkData = {
    class: Kafka,
    config: configs
  };
  streamBuffer.addSubscriber(streamLinkData);
  const mergedConfigs = Object.assign({}, configs, stream.settings);
  stream.subscribe(new Kafka(mergedConfigs));
  return streamLinkData;
};

module.exports.KafkaSubscriber = Kafka;
