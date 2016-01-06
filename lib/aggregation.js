'use strict';

var _ = require('lodash');

var internals = {};

internals.mappers = {
    terms: function(result, agg) {
        return result.buckets.reduce(function (acc, bucket) {
            var bucketValue = _.reduce(agg.aggs, function (acc, agg, key) {
                acc[key] = agg.map(bucket[key]);
                return acc;
            }, {});

            acc[bucket.key] = _.assign({}, { count: bucket.doc_count }, bucketValue );
            return acc;
        }, {});
    },
    sum: function(result) {
        return result.value;
    }
};

internals.mapper = function (aggType) {
    return aggType && internals.mappers[aggType] || _.noop;
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
Aggregation.sum = internals.wrap('sum');