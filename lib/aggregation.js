'use strict';

var _ = require('lodash');

var internals = {};

internals.bucketReducer = function(iterator) {
    iterator = iterator || _.identity;

    return function(result, agg) {
        return result.buckets.reduce(function (acc, bucket) {
            var value = _.reduce(agg.aggs, function (acc, agg, key) {
                acc[key] = agg.map(bucket[key]);
                return acc;
            }, {});
            acc[bucket.key] = iterator(value, bucket);
            return acc;
        }, {});
    };
};

internals.metricPlucker = function(iterator) {
    iterator = iterator || _.identity;

    return function(result) {
        return iterator(result.value);
    };
};

internals.mappers = {
    terms: internals.bucketReducer(function(value, bucket) {
        return _.assign({ count: bucket.doc_count }, value);
    }),
    sum: internals.metricPlucker(),
    avg: internals.metricPlucker(),
    min: internals.metricPlucker(),
    max: internals.metricPlucker(),
    value_count: internals.metricPlucker()
};

internals.mapper = function (aggType) {
    return aggType && internals.mappers[aggType] || _.identity;
};

internals.wrap = function(aggType, parse) {
    parse = parse || function(field) {
            return { field: field };
        };

    return function(body) {
        return _.set({}, aggType, _.isString(body) ? parse(body) : body);
    };
};

function Aggregation(body) {
    this.body = body || {};
    this.aggs = {};
}

Aggregation.prototype.build = function() {
    var payload = _.clone(this.body);
    if (_.keys(this.aggs).length) {
        payload.aggregations = _.mapValues(this.aggs, function (agg) {
            return agg.build();
        });
    }
    return payload;
};

Aggregation.prototype.visit = function(visitor) {
    if (_.isFunction(visitor)) visitor(this);

    return this;
};

Aggregation.prototype.aggregation = function(name, body, callback) {
    this.aggs[name] = new Aggregation(body)
        .mapper(internals.mapper(_.keys(body)[0]))
        .visit(callback);

    return this;
};

Aggregation.prototype.agg = Aggregation.prototype.aggregation;

Aggregation.prototype._map = function(result, agg) {
    return _.mapValues(agg.aggs, function (agg, key) {
        return agg.map(result.aggregations && result.aggregations[key]);
    });
};

Aggregation.prototype.map = function(result) {
    return this._map(result, this);
};

Aggregation.prototype.mapper = function(mapper) {
    this._map = mapper;
    return this;
};

exports.Aggregation = Aggregation;

Aggregation.terms = internals.wrap('terms');
Aggregation.stats = internals.wrap('stats');
Aggregation.sum = internals.wrap('sum');
Aggregation.avg = internals.wrap('avg');
Aggregation.max = internals.wrap('max');
Aggregation.min = internals.wrap('min');
Aggregation.valueCount = internals.wrap('value_count');