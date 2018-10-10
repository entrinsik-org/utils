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

const LODASH_MIXINS = [
    'all',
    'any',
    'at',
    'collect',
    'contains',
    'countBy',
    'detect',
    'each',
    'eachRight',
    'every',
    'filter',
    'find',
    'findLast',
    'findWhere',
    'foldl',
    'foldr',
    'forEach',
    'forEachRight',
    'groupBy',
    'include',
    'includes',
    'indexBy',
    'inject',
    'invoke',
    'map',
    'partition',
    'pluck',
    'reduce',
    'reduceRight',
    'reject',
    'sample',
    'select',
    'shuffle',
    'size',
    'some',
    'sortBy',
    'sortByAll',
    'sortByOrder',
    'where',
    'assign',
    'create',
    'defaults',
    'defaultsDeep',
    'extend',
    'findKey',
    'findLastKey',
    'forIn',
    'forInRight',
    'forOwn',
    'forOwnRight',
    'functions',
    'get',
    'has',
    'invert',
    'keys',
    'keysIn',
    'mapKeys',
    'mapValues',
    'merge',
    'methods',
    'omit',
    'pairs',
    'pick',
    'result',
    'set',
    'transform',
    'values',
    'valuesIn',
    'chunk',
    'compact',
    'difference',
    'drop',
    'dropRight',
    'dropRightWhile',
    'dropWhile',
    'fill',
    'findIndex',
    'findLastIndex',
    'first',
    'flatten',
    'flattenDeep',
    'head',
    'indexOf',
    'initial',
    'intersection',
    'last',
    'lastIndexOf',
    'object',
    'pull',
    'pullAt',
    'remove',
    'rest',
    'slice',
    'sortedIndex',
    'sortedLastIndex',
    'tail',
    'take',
    'takeRight',
    'takeRightWhile',
    'takeWhile',
    'union',
    'uniq',
    'unique',
    'unzip',
    'unzipWith',
    'without',
    'xor',
    'zip',
    'zipObject',
    'zipWith',
    'camelCase',
    'capitalize',
    'deburr',
    'endsWith',
    'escape',
    'escapeRegExp',
    'kebabCase',
    'pad',
    'padLeft',
    'padRight',
    'parseInt',
    'repeat',
    'snakeCase',
    'startCase',
    'startsWith',
    'template',
    'trim',
    'trimLeft',
    'trimRight',
    'trunc',
    'unescape',
    'words'
];

/**
 * This class enables easy building and parsing of recursive elasticsearch aggregation structures
 */
class Aggregation {
    constructor(props = {}) {
        _.assign(this, joi.attempt(props, schema));
        this.aggregations = [];
        this._thru = _.identity;
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
        this.aggregations = _.flatten(aggs.map(agg => agg instanceof Aggregation ? agg : Aggregation.aggs(agg)));
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
     * Sets a new mapping function
     * @param iter
     * @return {Aggregation}
     */
    thru(iter) {
        this._thru = _.flow(this._thru, iter);
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
     * Rounds a value agg
     * @return {Aggregation}
     */
    round(decimals = 0) {
        return this.thru(v => v > 0 ? Math.round(v * Math.pow(10, decimals)) / Math.pow(10, decimals) : 0);
    }

    /**
     * Picks values from a collection
     * @param args
     */
    multipick(...args) {
        return this.map(r => _.pick(r, ...args));
    }

    /**
     * Omits values from a collection
     * @param args
     * @return {*}
     */
    multiomit(...args) {
        return this.map(r => _.omit(r, ...args));
    }

    fields(...args) {
        return this.map(r => _.at(r, ...args));
    }

    indexByKey(iter) {
        iter = iter || _.get(this.aggregations[ 0 ], 'id', 'doc_count');
        const map = _.isFunction(iter) ? iter : v => _.get(v, iter);
        return this.reduce((acc, bucket) => _.set(acc, [ bucket.key ], map(bucket)), {});
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

    _transform(result, buckets) {
        // root agg default
        if (result.aggregations) return this.aggregations.reduce((acc, agg) => _.set(acc, [ agg.id ], agg.transform(result.aggregations[ agg.id ])), {});

        // multi bucket agg default (e.g. terms)
        if (result.buckets) return result.buckets.map(bucket => this.transformBucket(buckets, bucket));

        // metric agg default
        if (result.value) return result.value;

        // single bucket agg default (e.g. filter)
        if (this.aggregations) return this.aggregations.reduce((acc, agg) => _.set(acc, [ agg.id ], agg.transform(result[ agg.id ])), {});

        // any other agg format
        return result;
    }

    /**
     * Transforms the elasticsearch agg response into something more useful. This allows the client to specify
     * reducers throughout the agg tree
     * @param result
     * @param buckets
     * @return {*}
     */
    transform(result, buckets) {
        return this._thru(this._transform(result, buckets), buckets);
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

    /**
     * Creates an agg array out of an object literal, whose keys will become the agg ids
     * @param aggObj
     * @return {Array}
     */
    static aggs(aggObj) {
        return _.map(aggObj, (value, key) => value.as(key));
    }
}

/**
 * Adds utility methods to aggregation prototype for transformation
 * @param methods
 */
function mixin(methods) {
    _.defaults(Aggregation.prototype, methods);
}

const mixins = LODASH_MIXINS.reduce((acc, method) => _.set(acc, method, function (...args) {
    return this.thru(v => _[method](v, ...args));
}), {});

_.defaults(Aggregation.prototype, mixins);

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

const bucketAggBuilders = BUCKET_AGGS.reduce((acc, agg) => _.set(acc, _.camelCase(agg), config => new Aggregation({
    body: fieldBodyBuilder(agg, config)
})), {});

const metricAggBuilders = METRIC_AGGS.reduce((acc, agg) => _.set(acc, _.camelCase(agg), config => new Aggregation({
    body: fieldBodyBuilder(agg, config)
})), {});

const pipelineAggBuilders = PIPELINE_AGGS.reduce((acc, agg) => _.set(acc, _.camelCase(agg), config => new Aggregation({
    body: pipelineBodyBuilder(agg, config)
})), {});

const matrixAggBuilders = MATRIX_AGGS.reduce((acc, agg) => _.set(acc, _.camelCase(agg), config => new Aggregation({
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
function aggregation(opts) {
    return new Aggregation(opts);
}

_.assign(
    module.exports,
    { Aggregation, aggregation, docCount, mixin },
    bucketAggBuilders,
    metricAggBuilders,
    pipelineAggBuilders,
    matrixAggBuilders
);