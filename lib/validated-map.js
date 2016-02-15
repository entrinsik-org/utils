'use strict';

var _ = require('lodash');
var joi = require('joi');

function ValidatedMap(schema) {
    this._schema = schema;
    this._values = {};
}

ValidatedMap.prototype.get = function(key) {
    return key ? this._values[key] : _.clone(this._values);
};

ValidatedMap.prototype.set = function(key, value) {
    var self = this;

    joi.validate(value, this._schema, function (err, validated) {
        if (err) throw err;

        self._values[key] = validated;
    });
};

ValidatedMap.prototype.has = function(key) {
    return this._values.hasOwnProperty(key);
};

ValidatedMap.prototype.keys = function() {
    return Object.keys(this._values);
};

ValidatedMap.prototype.values = function() {
    return _.values(this._values);
};

ValidatedMap.prototype.map = function(iteratee, receiver) {
    return _.map(this._values, iteratee, receiver);
};

ValidatedMap.prototype.reduce = function(accumulator, iteratee, initialValue, receiver) {
    return _.reduce(this._values, iteratee, initialValue, receiver);
};

exports.ValidatedMap = ValidatedMap;