'use strict';

var Collection = require('./collection').Collection;
var ValidatedMap = require('./validated-map').ValidatedMap;
var _ = require('lodash');
var slice = exports.slice = Function.call.bind(Array.prototype.slice);

exports.collection = function (items, start, total) {
    return new Collection(items, start, total);
};

exports.cork = function (fn) {
    var corked = true;
    var invocations = [];

    var corkFn = function () {
        if (corked) {
            invocations.push([this].concat(slice(arguments)));
        } else {
            return fn.apply(this, arguments);
        }
    };

    corkFn.cork = function () {
        corked = true;
    };

    corkFn.uncork = function () {
        corked = false;
        invocations.forEach(function (invocation) {
            fn.apply(invocation.shift(), invocation);
        });
        invocations = [];
    };

    return corkFn;
};

exports.validatedMap = function (schema) {
    return new ValidatedMap(schema);
};

exports.shorthand = function (value, mapper) {
    var defaultMapper = function (key, value) {
        return { key: key, value: value };
    };

    mapper = mapper || defaultMapper;

    value = value || {};


    if (_.isString(value)) value = _.set({}, value, null);

    return _.reduce(value, function (acc, value, key) {
        return mapper(key, value);
    }, null);
};

exports.alias = function () {
    return _([].slice.call(arguments))
        .compact()
        .map(_.camelCase)
        .value()
        .join('-');
};

exports.singlePlural = function (value, single, plural) {
    return value === 1 ? single : plural;
};

exports.jsonPatchSchema = require('./json-patch-schema');

exports.Aggregation = require('./aggregation').Aggregation;
exports.AggregateExpression = require('./aggregate-expression').AggregateExpression;
exports.ElasticQueryTraverser = require('./elastic-query-traverser').ElasticQueryTraverser;
exports.DateKeywords = require('./date-keywords').DateKeywords;

exports.exists = function (value, exception) {
    if (_.isNull(value) || _.isUndefined(value)) throw exception;

    return value;
};