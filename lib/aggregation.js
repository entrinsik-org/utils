'use strict';

var _ = require('lodash');

function Aggregation() {
    this.body = {};
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

Aggregation.prototype._aggregation = function(type, body) {
    this.body[type] = body;
    return this;
};

Aggregation.prototype.terms = function (body) {
    return this._aggregation('terms', body)
        .mapper(function (result, agg) {
            return result.buckets.reduce(function (acc, bucket) {
                var bucketValue = _.reduce(agg.aggs, function (acc, agg, key) {
                    acc[key] = agg.map(bucket[key]);
                    return acc;
                }, {});

                acc[bucket.key] = _.assign({}, { count: bucket.doc_count }, bucketValue );
                return acc;
            }, {});
        });
};

Aggregation.prototype.sum = function (body) {
    return this._aggregation('sum', body);
};

Aggregation.prototype.aggregation = function(name, body) {
    var agg = this.aggs[name] = new Aggregation(this);
    if (_.isFunction(body)) {
        body(agg);
    } else {
        agg.body = body;
    }
    return this;
};

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