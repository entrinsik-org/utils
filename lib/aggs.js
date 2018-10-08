'use strict';

const _ = require('lodash');
const joi = require('joi');

let nextId = 0;

const schema = joi.object().keys({
    id: joi.string().default(() => `agg_${nextId++}`, 'default id'),
    body: joi.object(),
    toJson: joi.func(),
    transform: joi.func(),
    reduce: joi.func()
});

const METRIC_AGGS = [
    'avg',
    'cardinality',
    'extended_stats',
    'geo_bounds',
    'geo_centroid',
    'max',
    'min',
    'percentiles',
    'percentile_ranks',
    'scripted_metric',
    'stats',
    'sum',
    'top_hits',
    'value_count'
];

const BUCKET_AGGS = [
    'adjacency_matrix',
    'children',
    'date_histogram',
    'date_range',
    'diversified_sampler',
    'filter',
    'filters',
    'geo_distance',
    'geohash_grid',
    'global',
    'histogram',
    'ip_range',
    'missing',
    'nested',
    'range',
    'reverse_nested',
    'sampler',
    'significant_terms',
    'terms'
];

const PIPELINE_AGGS = [
    'avg_bucket',
    'derivative',
    'max_bucket',
    'min_bucket',
    'sum_bucket',
    'stats_bucket',
    'extended_stats_bucket',
    'percentiles_bucket',
    'moving_avg',
    'cumulative_sum',
    'bucket_script',
    'bucket_selector',
    'serial_diff'
];

const MATRIX_AGGS = [
    'matrix_stats'
];

/**
 * This class enables easy building and parsing of recursive elasticsearch aggregation structures
 */
class Aggregation {
    constructor(props = {}) {
        _.assign(this, joi.attempt(props, schema));
        this.aggregations = [];
    }

    /**
     * Sets the id of the aggregation.
     * @param id
     * @return {Aggregation}
     */
    as(id) {
        this.id = id;
        return this;
    }

    /**
     * Replaces the child aggs with a new set of aggs
     * @param aggs one or more agg instances or agg configs
     * @return {Aggregation}
     */
    aggs(...aggs) {
        this.aggregations = aggs.map(agg => agg instanceof Aggregation ? agg : new Aggregation(agg));
        return this;
    }

    /**
     * Adds one or more aggs to the current set of aggs
     * @param aggs
     * @return {Aggregation}
     */
    agg(...aggs) {
        return this.aggs(...[ ...this.aggregations, ...aggs ]);
    }

    /**
     * Recursively nests aggs into an aggregation tree. Arguments may be arrays of aggs which are nested as siblings and
     * inherit all descendent aggs
     * @param aggs
     * @return {Aggregation}
     * @example
     * JSON.stringify(new Aggregation()
     *   .nest(
     *       Aggregation.terms('ShipCountry').as('countries').mapper(b => ({ country: b.key, cities: b.cities })),
     *       Aggregation.terms('ShipCity').as('cities').mapper(b => ({ city: b.key, averages: b.averages, totals: b.totals })),
     *       [ Aggregation.sum('orderAmount').as('totals'), Aggregation.sum('orderAmount').as('averages') ]
     *   )
     *);
     *
     */
    nest(...aggs) {
        const [ children, ...grandchildren ] = aggs;

        return children ?
            this.aggs(...[].concat(children).map(child => child.nest(...grandchildren))) :
            this;
    }

    /**
     * Renders the json tree to send to elasticsearch
     * @return {*}
     */
    toJson() {
        // virtual agg
        return _.chain(this.aggregations)
            .filter(agg => agg.body)
            .reduce((acc, agg) => _.set(acc, [ 'aggs', agg.id ], agg.toJson()), {})
            .assign(this.body)
            .value();
    }

    /**
     * Sets a bucket mapper transformation function (for bucketed aggs only). The transformed result of the aggregation will be an array
     * with the iterator function applied for each bucket. Any child aggs will be transformed prior to the iterator being called.
     * @param iter {function} an iterator function
     * @return {Aggregation}
     */
    mapper(iter) {
        this.transform = (result, buckets) => {
            return result.buckets.map(bucket => iter(this.transformBucket(buckets, bucket)));
        };
        return this;
    }

    /**
     * Sets a bucket reducer transformation function (for bucketed aggs only).
     * @param iter {function} the reduction accumulator
     * @param initValue {*} the initial value to use in the reduction
     * @return {Aggregation}
     */
    reducer(iter, initValue) {
        this.transform = (result, buckets) => {
            return result.buckets.reduce((acc, bucket) => iter(acc, this.transformBucket(buckets, bucket)), initValue);
        };
        return this;
    }

    /**
     * Transforms a single bucket. Used internally during transformation
     * @param buckets { Array } an array of parent buckets
     * @param bucket
     * @return {*}
     */
    transformBucket(buckets = [], bucket) {
        // push this bucket onto the bucket list
        return this.aggregations.reduce((acc, agg) => _.set(bucket, agg.id, agg.transform(bucket[ agg.id ], [ ...buckets, bucket ])), bucket);
    }

    /**
     * Sets a transformation function for the agg
     * @param transform
     * @return {Aggregation}
     */
    transformer(transform) {
        this.transform = transform;
        return this;
    }

    /**
     * Transforms the elasticsearch agg response into something more useful. This allows the client to specify
     * reducers throughout the agg tree
     * @param result
     * @param buckets
     * @return {*}
     */
    transform(result, buckets) {
        if (result.buckets) return result.buckets.map(bucket => this.transformBucket(buckets, bucket));
        if (result.aggregations) return this.aggregations.reduce((acc, agg) => _.set(acc, [ agg.id ], agg.transform(result.aggregations[ agg.id ])), {});
        if (result.value) return result.value;
        return result;
    }

    /**
     * Sends the search to elastic and transforms the result
     * @param client
     * @param opts
     * @return {Promise<*>}
     */
    async search(client, opts) {
        return this.transform(await client.search(_.merge({ size: 0 }, opts, { body: this.toJson() })));
    }
}

/**
 * Higher order function to create an agg body with an optional default property
 * @type {Function}
 */
const bodyBuilder = _.curry(function (defaultField, type, config) {
    config = _.isString(config) ? { [ defaultField ]: config } : config;
    return { [ type ]: config };
});

// for aggs whose default single argument should be { field: <arg> }
const fieldBodyBuilder = bodyBuilder('field');

// for aggs whose default single argument should be { buckets_path: <arg> }
const pipelineBodyBuilder = bodyBuilder('buckets_path');

const bucketAggBuilders = BUCKET_AGGS.reduce((acc, agg) => _.set(acc, agg, config => new Aggregation({
    body: fieldBodyBuilder(agg, config)
})), {});

const metricAggBuilders = METRIC_AGGS.reduce((acc, agg) => _.set(acc, agg, config => new Aggregation({
    body: fieldBodyBuilder(agg, config)
})), {});

const pipelineAggBuilders = PIPELINE_AGGS.reduce((acc, agg) => _.set(acc, agg, config => new Aggregation({
    body: pipelineBodyBuilder(agg, config)
})), {});

const matrixAggBuilders = MATRIX_AGGS.reduce((acc, agg) => _.set(acc, agg, config => new Aggregation({
    body: { [ agg ]: config }
})), {});

/**
 * A virtual doc count agg that peeks at its parent bucket
 * @return {Aggregation}
 */
function docCount() {
    return new Aggregation({
        transform: (result, buckets) => _.get(buckets.pop(), 'doc_count')
    });
}

/**
 * Creates a top level agg
 * @return {Aggregation}
 */
function create(opts) {
    return new Aggregation(opts);
}

_.assign(
    module.exports,
    { Aggregation, create, docCount },
    bucketAggBuilders,
    metricAggBuilders,
    pipelineAggBuilders,
    matrixAggBuilders
);