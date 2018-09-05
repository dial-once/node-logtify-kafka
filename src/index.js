const { Producer, Client } = require('kafka-node');
const logtify = require('logtify');

const { streamBuffer } = logtify;
const { stream, adapters } = logtify();

class Kafka extends stream.Subscriber {
  constructor(configs) {
    super();
    this.ready = false;
    this.settings = configs || {};
    if (this.settings.KAFKA_URL && this.settings.KAFKA_TOPIC) {
      const kafkaClient = new Client({
        connectionString: this.settings.KAFKA_URL,
      });
      this.producer = new Producer(kafkaClient);
      this.producer.once('ready', () => {
        this.ready = true;
        this.producer.createTopics([this.settings.KAFKA_TOPIC], false, () => {});
      });
      this.producer.on('error', (e) => {
        const defaultLogger = adapters.get('logger');
        if (defaultLogger && defaultLogger.winston) {
          defaultLogger.winston.error(e);
        }
      });
    } else {
      console.warn('Kafka logtify module is not active due to a missing KAFKA_URL and KAFKA_TOPIC');
    }

    this.cleanup = this.cleanup.bind(this);
    this.name = 'KAFKA';
  }

  isEnabled() {
    const result = ['true', 'false'].includes(process.env.KAFKA_LOGGING)
      ? process.env.KAFKA_LOGGING === 'true'
      : this.settings.KAFKA_LOGGING;
    // enabled by default
    return [null, undefined].includes(result) ? true : result;
  }

  handle(message) {
    if (this.ready && message) {
      const content = message.payload;
      const messageLevel = this.logLevels.has(content.level) ? content.level : this.logLevels.get('default');
      const minLogLevel = this.getMinLogLevel(this.settings, this.name);
      if (this.logLevels.get(messageLevel) >= this.logLevels.get(minLogLevel)) {
        const metadata = message.stringifyMetadata();
        this.producer.send({
          topic: this.settings.KAFKA_TOPIC,
          messages: {
            text: content.text,
            prefix: JSON.stringify(message.getPrefix(this.settings)),
            metadata
          }
        });
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
