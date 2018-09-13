const assert = require('assert');
const sinon = require('sinon');
const { stream } = require('logtify')({});
const serializeError = require('serialize-error');
const Kafka = require('../src/index.js');

const { Message } = stream;

describe('Kafka plugin', () => {
  before(() => {
    delete process.env.KAFKA_HOST;
    delete process.env.KAFKA_TOPIC;
  });

  afterEach(() => {
    delete process.env.KAFKA_HOST;
    delete process.env.KAFKA_TOPIC;
    delete process.env.KAFKA_LOGGING;
    delete process.env.MIN_LOG_LEVEL;
    delete process.env.MIN_LOG_LEVEL_KAFKA;
    delete process.env.KAFKA_CONNECT_TIMEOUT;
    delete process.env.KAFKA_REQUEST_TIMEOUT;
  });

  it('should return configs and a constructor', () => {
    const kafkaPackage = Kafka();
    assert.equal(typeof kafkaPackage, 'object');
    assert.deepEqual(kafkaPackage.config, { KAFKA_HOST: undefined, KAFKA_TOPIC: undefined });
    assert.equal(typeof kafkaPackage.class, 'function');
    assert(kafkaPackage.class, Kafka.KafkaSubscriber);
  });

  it('should return given configs and a constructor', () => {
    const configs = { KAFKA_HOST: 'test', KAFKA_TOPIC: 'testTopic' };
    const kafkaPackage = Kafka(configs);
    assert.equal(typeof kafkaPackage, 'object');
    assert.deepEqual(kafkaPackage.config, configs);
    assert.equal(typeof kafkaPackage.class, 'function');
  });

  it('should not throw if no settings are given', () => {
    const kafka = new Kafka.KafkaSubscriber({});
    assert.equal(kafka.kafkaClient, undefined);
    assert.equal(kafka.kafkaSubscriber, undefined);
  });

  it('should expose its main functions', () => {
    const kafka = new Kafka.KafkaSubscriber({});
    assert.equal(typeof kafka, 'object');
    assert.equal(kafka.name, 'KAFKA');
    assert.equal(typeof kafka.cleanup, 'function');
    assert.equal(typeof kafka.isEnabled, 'function');
    assert.equal(typeof kafka.handle, 'function');
  });

  it('should not be initialized if no url is provided', () => {
    const spy = sinon.spy(console, 'warn');
    const kafka = new Kafka.KafkaSubscriber({ KAFKA_TOPIC: 'testTopic' });
    assert(spy.calledWith('Kafka logtify module is not active due to a missing KAFKA_HOST and/or KAFKA_TOPIC'));
    assert.equal(kafka.kafkaClient, undefined);
    assert.equal(kafka.kafkaProducer, undefined);
    assert.equal(kafka.ready, false);
    spy.restore();
  });

  it('should not be initialized if no topic was provided', () => {
    const spy = sinon.spy(console, 'warn');
    const kafka = new Kafka.KafkaSubscriber({ KAFKA_HOST: 'test' });
    assert(spy.calledWith('Kafka logtify module is not active due to a missing KAFKA_HOST and/or KAFKA_TOPIC'));
    assert.equal(kafka.kafkaClient, undefined);
    assert.equal(kafka.kafkaProducer, undefined);
    assert.equal(kafka.ready, false);
    spy.restore();
  });

  it('should be initialized if all configs were provided', () => {
    const kafka = new Kafka.KafkaSubscriber({ KAFKA_TOPIC: 'testTopic', KAFKA_HOST: 'test' });
    assert.equal(typeof kafka.kafkaClient, 'object');
    assert.equal(typeof kafka.kafkaProducer, 'object');
  });

  it('should be ready if al configs provided [env]', () => {
    process.env.KAFKA_HOST = 'test';
    process.env.KAFKA_TOPIC = 'testTopic';
    const subscriber = Kafka({});
    const kafka = new Kafka.KafkaSubscriber(subscriber.config);
    assert.equal(typeof kafka.kafkaClient, 'object');
    assert.equal(typeof kafka.kafkaProducer, 'object');
  });

  it('should indicate if it is switched on/off [settings]', () => {
    let kafka = new Kafka.KafkaSubscriber({ KAFKA_LOGGING: true });
    assert.equal(kafka.isEnabled(), true);
    kafka = new Kafka.KafkaSubscriber({ KAFKA_LOGGING: false });
    assert.equal(kafka.isEnabled(), false);
    kafka = new Kafka.KafkaSubscriber(null);
    assert.equal(kafka.isEnabled(), true);
  });

  it('should indicate if it is switched on/off [envs]', () => {
    const kafka = new Kafka.KafkaSubscriber(null);
    assert.equal(kafka.isEnabled(), true);
    process.env.KAFKA_LOGGING = true;
    assert.equal(kafka.isEnabled(), true);
    process.env.KAFKA_LOGGING = false;
    assert.equal(kafka.isEnabled(), false);
  });

  it('should indicate if it is switched on/off [envs should have more privilege]', () => {
    const kafka = new Kafka.KafkaSubscriber({ KAFKA_LOGGING: true });
    assert.equal(kafka.isEnabled(), true);
    process.env.KAFKA_LOGGING = false;
    assert.equal(kafka.isEnabled(), false);
    process.env.KAFKA_LOGGING = undefined;
    assert.equal(kafka.isEnabled(), true);
  });

  it('should not break down if null is notified', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_LOGGING: true,
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic'
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    kafka.handle(null);
  });

  it('should log message if KAFKA_LOGGING = true', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_LOGGING: true,
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic'
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    const spy = sinon.spy(kafka.kafkaProducer, 'send');
    const message = new Message();
    kafka.handle(message);
    assert(spy.called);
    spy.restore();
  });

  it('should not log message if KAFKA_LOGGING = false', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_LOGGING: false,
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic'
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    const spy = sinon.spy(kafka.kafkaProducer, 'send');
    const message = new Message();
    kafka.handle(message);
    assert(!spy.called);
    spy.restore();
  });

  it('should accept additional settings parameters as one of the settings', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_LOGGING: false,
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic',
      KAFKA_CONNECT_TIMEOUT: 1000,
      KAFKA_REQUEST_TIMEOUT: 1000
    });
    assert.equal(kafka.kafkaClient.options.connectTimeout, 1000);
    assert.equal(kafka.kafkaClient.options.requestTimeout, 1000);
  });

  it('should accept additional settings parameters as one of the settings [env]', () => {
    process.env.KAFKA_CONNECT_TIMEOUT = 1000;
    process.env.KAFKA_REQUEST_TIMEOUT = 1000;
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_LOGGING: false,
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic'
    });
    assert.equal(kafka.kafkaClient.options.connectTimeout, 1000);
    assert.equal(kafka.kafkaClient.options.requestTimeout, 1000);
  });

  it('should not log if message level < MIN_LOG_LEVEL [settings]', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_LOGGING: true,
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic',
      MIN_LOG_LEVEL: 'error'
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    const spy = sinon.spy(kafka.kafkaProducer, 'send');
    const message = new Message();
    kafka.handle(message);
    assert(!spy.called);
    spy.restore();
  });

  it('should not log if message level < MIN_LOG_LEVEL [envs]', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_LOGGING: true,
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic'
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    process.env.MIN_LOG_LEVEL = 'warn';
    const spy = sinon.spy(kafka.kafkaProducer, 'send');
    const message = new Message();
    kafka.handle(message);
    assert(!spy.called);
    spy.restore();
  });

  it('should log if message level >= MIN_LOG_LEVEL_KAFKA but < MIN_LOG_LEVEL [envs]', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic',
      KAFKA_LOGGING: true
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    const spy = sinon.spy(kafka.kafkaProducer, 'send');
    const message = new Message('warn');
    process.env.MIN_LOG_LEVEL = 'error';
    process.env.MIN_LOG_LEVEL_KAFKA = 'warn';
    kafka.handle(message);
    assert(spy.called);
    spy.restore();
  });

  it('should log if message level = MIN_LOG_LEVEL [envs]', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic',
      KAFKA_LOGGING: true
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    const spy = sinon.spy(kafka.kafkaProducer, 'send');
    const message = new Message('error');
    process.env.MIN_LOG_LEVEL = 'error';
    kafka.handle(message);
    assert(spy.called);
    spy.restore();
  });

  it('should log if message level > MIN_LOG_LEVEL [envs]', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic',
      KAFKA_LOGGING: true
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    const spy = sinon.spy(kafka.kafkaProducer, 'send');
    const message = new Message('error');
    process.env.MIN_LOG_LEVEL = 'warn';
    kafka.handle(message);
    assert(spy.called);
    spy.restore();
  });

  it('should correctly process a message', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic',
      KAFKA_LOGGING: true,
      JSONIFY: true
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    const spy = sinon.spy(kafka.kafkaProducer, 'send');
    const message = new Message('error', 'Hello world', { hello: 'world', one: 1, two: '2' });
    kafka.handle(message);
    assert(spy.called);
    assert.deepEqual(spy.args[0][0],
      [{
        topic: 'testTopic',
        messages: {
          level: 'error',
          text: 'Hello world',
          prefix: {
            timestamp: '',
            environment: '',
            logLevel: '',
            reqId: '',
            isEmpty: true
          },
          metadata: {
            instanceId: message.payload.meta.instanceId,
            hello: 'world',
            one: 1,
            two: '2'
          }
        },
        partition: 0,
        attributes: 0
      }]);
    spy.restore();
  });

  it('should correctly handle error sent as a message [msg body]', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic',
      KAFKA_LOGGING: true,
      JSONIFY: true
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    const spy = sinon.spy(kafka.kafkaProducer, 'send');
    const error = new Error('Sprint plan was modified');
    const message = new Message('error', error, { hello: 'world', one: 1, two: '2' });
    kafka.handle(message);
    assert(spy.called);
    assert.deepEqual(spy.args[0][0],
      [{
        topic: 'testTopic',
        messages: {
          level: 'error',
          text: message.payload.text,
          prefix: {
            timestamp: '',
            environment: '',
            logLevel: '',
            reqId: '',
            isEmpty: true
          },
          metadata: {
            error: serializeError(error),
            instanceId: message.payload.meta.instanceId,
            hello: 'world',
            one: 1,
            two: '2'
          }
        },
        partition: 0,
        attributes: 0
      }]);
    spy.restore();
  });

  it('should correctly handle error sent as a message metadata [msg body]', () => {
    const kafka = new Kafka.KafkaSubscriber({
      KAFKA_HOST: 'test',
      KAFKA_TOPIC: 'testTopic',
      KAFKA_LOGGING: true,
      JSONIFY: true
    });
    // setting manually because there is no broker and 'ready' even will never be fired
    kafka.ready = true;
    const spy = sinon.spy(kafka.kafkaProducer, 'send');
    const error = new Error('Sprint plan was modified');
    const message = new Message('error', 'OMG!!!', {
      error,
      hello: 'world',
      one: 1,
      two: '2'
    });
    kafka.handle(message);
    assert(spy.called);
    assert.deepEqual(spy.args[0][0],
      [{
        topic: 'testTopic',
        messages: {
          level: 'error',
          text: 'OMG!!!',
          prefix: {
            timestamp: '',
            environment: '',
            logLevel: '',
            reqId: '',
            isEmpty: true
          },
          metadata: {
            error: serializeError(error),
            instanceId: message.payload.meta.instanceId,
            hello: 'world',
            one: 1,
            two: '2'
          }
        },
        partition: 0,
        attributes: 0
      }]);
    spy.restore();
  });
});
