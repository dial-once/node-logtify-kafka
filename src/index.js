const { Producer, KafkaClient } = require('kafka-node');
const logtify = require('logtify');

const { streamBuffer } = logtify;
const { stream } = logtify();

/**
 * Kafka plugin for logtify
 */
class Kafka extends stream.Subscriber {
  constructor(configs) {
    super();
    this.ready = false;
    this.settings = configs || {};
    this.kafkaClient = undefined;
    this.kafkaProducer = undefined;

    this.defaultLogger = stream.adapters.get('logger');
    if (this.settings.KAFKA_HOST && this.settings.KAFKA_TOPIC) {
      this.initialize();
    } else {
      (this.defaultLogger || console)
        .warn('Kafka logtify module is not active due to a missing KAFKA_HOST and/or KAFKA_TOPIC');
    }

    this.cleanup = this.cleanup.bind(this);

    process.once('exit', this.cleanup);
    process.once('SIGINT', this.cleanup);
    process.once('SIGTERM', this.cleanup);
    process.once('uncaughtException', this.cleanup);

    this.name = 'KAFKA';
  }

  /**
   * Initialize kafka client, producer and connect them to kafka broker
   * @return void
   */
  initialize() {
    this.kafkaClient = new KafkaClient({
      kafkaHost: this.settings.KAFKA_HOST,
      connectTimeout: process.env.KAFKA_CONNECT_TIMEOUT || this.settings.KAFKA_CONNECT_TIMEOUT || 5000,
      requestTimeout: process.env.KAFKA_REQUEST_TIMEOUT || this.settings.KAFKA_REQUEST_TIMEOUT || 10000,
      autoConnect: true
    });
    this.kafkaProducer = new Producer(this.kafkaClient);
    this.kafkaProducer.once('ready', () => {
      this.ready = true;
      this.kafkaProducer.createTopics([this.settings.KAFKA_TOPIC], false, () => {});
    });
    this.kafkaProducer.on('error', (e) => {
      (this.defaultLogger || console).error(e);
    });
  }

  /**
   * Check if this particular plugin is enabled
   * @return {Boolean} - the status of the plugin
   */
  isEnabled() {
    const result = ['true', 'false'].includes(process.env.KAFKA_LOGGING)
      ? process.env.KAFKA_LOGGING === 'true'
      : this.settings.KAFKA_LOGGING;
    // enabled by default
    return [null, undefined].includes(result) ? true : result;
  }

  /**
   * A function that is called when the plugin is unlinked or the project is closed
   * @return {void}
   */
  cleanup() {
    if (this.kafkaClient) {
      this.kafkaClient.close();
      this.kafkaProducer.removeAllListeners('error');
    }
  }

  /**
   * Format and send the given message to the kafka broker
   * @param  {Object} message - and instance of a Message class exposed by the logtify module.
   * @return {void}
   */
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

/**
 * Configuration function. Receives plugin-specific settings
 * @param  {Object} config - configurations for this particular plugin
 * @return {Object} - { class: Kafka, config: object }
 */
module.exports = (config) => {
  const configs = Object.assign({
    KAFKA_HOST: process.env.KAFKA_HOST,
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
