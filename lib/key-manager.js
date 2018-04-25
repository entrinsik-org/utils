'use strict';

const uuid = require('node-uuid');

module.exports = {

    generate: function () {
        return uuid.v1();
    },

    cleanup: function (client, key) {
        return client.delAsync(key);
    },

    decoreateQueryStream: function (stream, client, key){
        stream.on('cancel', () => {
            this.cleanup(client, key);
        });

        stream.on('error', () => {
            this.cleanup(client, key);
        });
    },

    decorateFlowStream: function (stream, client, key) {

        stream.on('cancel', () => {
            this.cleanup(client, key);
        });

        stream.on('error', () => {
            this.cleanup(client, key);
        });
        stream.on('end', () => {
            this.cleanup(client, key);
        });

    }

};