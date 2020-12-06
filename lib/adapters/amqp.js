const amqplib = require('amqplib');
const debug = require('debug')('feathers-sync:amqp');
const core = require('../core');

module.exports = config => {


  debug(`Setting up AMQP connection ${config.uri}`);

  return app => {
    initAmqp(app, config);
  };
};

const initAmqp = async (app, config) => {
  try {
    await new Promise((resolve, reject) => {
      app.configure(core);
      const open = amqplib.connect(config.uri, config.amqpConnectionOptions);
      const {key = 'feathers-sync'} = config;
      const {deserialize, serialize} = config;
      const ready = open.then((connection) => {
        connection.createChannel().then(channel => {
          return channel.assertExchange(key, 'fanout', {durable: false}).then(() => {
            return channel.assertQueue('', {autoDelete: true}).then(q => {
              console.log('channel asserted');
              channel.bindQueue(q.queue, key, '').then(() => {
                channel.consume(q.queue, message => {
                  if (message !== null) {
                    debug(`Got ${key} event from APMQ channel`);
                    app.emit('sync-in', message.content);
                  }
                }, {noAck: true});

                app.on('sync-out', data => {
                  debug(`Publishing ${key} event to APMQ channel`);
                  try {
                    channel.publish(key, '', Buffer.from(data));
                  } catch (e) {
                  }
                });

                return channel;
              });
            });
          });
        }).then(channel => ({connection, channel}));
        connection.on('error', (e) => {
          reject(e);
        })
      }).catch((e) => {
        reject(e);
      });

      app.sync = {
        deserialize,
        serialize,
        ready,
        type: 'amqp'
      };
    });
  } catch (e) {
    setTimeout(() => {
      initAmqp(app, config);
    }, 1000);
  }
};
