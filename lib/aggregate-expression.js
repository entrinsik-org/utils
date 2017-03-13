'use strict';

/**
 * Parser and other utilities for aggregate expression of the format 'fn[:field]'
 *
 * Valid fields are any objects with properties { name: string, label: string, dataType: string('date', 'number',...) }
 * Data types are checked for values 'date', 'number'
 */

const _ = require('lodash');
const joi = require('joi');
const Aggregation = require('./aggregation').Aggregation;

const COUNT = 'count', SUM = 'sum', AVG = 'avg', MAX = 'max', MIN = 'min', VALUE_COUNT = 'value_count', CARDINALITY = 'cardinality';

const defaultMapper = (label, value) => ({ label: label, value: value });

var internals = {};

/**
 * Generates a list of valid aggregate expressions for a collection of fields
 * @param fields
 * @return {*}
 */
internals.valids = function (fields) {
    const SAFE_AGGS = [COUNT]
        .concat(_.map(fields, f => `${VALUE_COUNT}:${f.name}`))
    .concat(_.map(fields, f => `${CARDINALITY}:${f.name}`));

    return _(fields)
            .filter(f => f.dataType === 'number' || f.dataType === 'date')
    .reduce(function (acc, f) {
        acc.push(`${MAX}:${f.name}`);
        acc.push(`${MIN}:${f.name}`);

        if (f.dataType == 'number') {
            acc.push(`${SUM}:${f.name}`);
            acc.push(`${AVG}:${f.name}`);
        }

        return acc;
    }, SAFE_AGGS);
};

/**
 * Represents an aggregate expression
 * @param {'count'|'sum'|'avg'|'max'|'min'} fn the aggregate function
 * @param {{ name: string, label: string, dataType: string}=} field the field being aggregated
 * @constructor
 */
function AggregateExpression (fn, field) {
    this.fn = fn;
    this.field = field;
}

/**
 * Returns a label for the expression
 * @return {*}
 */
AggregateExpression.prototype.label = function () {
    switch (this.fn) {
        case COUNT:
            return 'Count';
        case SUM:
            return 'Total ' + this.field.label;
        case AVG:
            return 'Average ' + this.field.label;
        case MAX:
            return 'Max ' + this.field.label;
        case MIN:
            return 'Min ' + this.field.label;
        case VALUE_COUNT:
            return 'Value Count ' + this.field.label;
        case CARDINALITY:
            return 'Distinct Count ' + this.field.label;
        default:
            return 'Unknown';
    }
};

/**
 * If present, returns the field's label. Undefined otherwise
 * @return {string}
 */
AggregateExpression.prototype.fieldLabel = function () {
    return this.field && this.field.label;
};

/**
 * Returns the shorthand expression (e.g. 'sum:amount')
 * @return {string}
 */
AggregateExpression.prototype.toString = function () {
    return this.fn === COUNT ? COUNT : `${this.fn}:${this.field.name}`;
};

/**
 * Configures an aggregation to sort by the aggregate expression (which must be added separately)
 * @param agg the Aggregation to sort
 * @param {string=} direction the direction to sort (defaults to 'desc')
 * @return {*}
 */
AggregateExpression.prototype.sort = function (agg, direction) {
    if (this.fn !== COUNT) agg.configure({ order: { [this.aggName()]: direction || 'desc' } });
    return this;
};

/**
 * Returns an ES-friendly aggregation name (e.g. 'sum-amount')
 * @returns {string}
 */
AggregateExpression.prototype.aggName = function () {
    return this.fn === COUNT ? COUNT : `${this.fn}-${this.field.name}`;
}

/**
 * Applies an aggregate expression to an aggregation. Adds an ES aggregation to the query payload and configures
 * a mapper to parse the result
 * @param agg the Aggregation to add the expression to
 * @param {function=} mapper a mapping function that accepts (label, value). By default a {label: string, value: number} tuple will be used
 * @return {*}
 */
AggregateExpression.prototype.apply = function (agg, mapper) {
    let aggId = this.aggName();

    mapper = mapper || defaultMapper;

    if (agg.body.missing) {
        var missingFn = this.fn;
        if (missingFn !== COUNT) {
            agg.aggregation(aggId, { [this.fn]: { field: this.field.name } })
                .mapper(function (label) {
                    return { count: label.doc_count, value: label[aggId].value };
                });
        } else {
            agg.mapper(function (label) {
                return { count: label.doc_count, value: label.doc_count };
            });
        }
    } else {

        if (this.fn !== COUNT) {
            agg.aggregation(aggId, { [this.fn]: { field: this.field.name } })
                .mapper(Aggregation.bucketMapper((b, v) => mapper(b.key, v[aggId])));

        } else {
            agg.mapper(Aggregation.bucketMapper(b => mapper(b.key, b.doc_count)));
        }
    }

    return this;
};

/**
 * Parses a string shorthand into an AggregateExpression
 * @param fields an array or hash of fields
 * @param {string} expr an aggregate expression string
 * @return {AggregateExpression}
 */
AggregateExpression.parse = function (fields, expr) {
    joi.assert(expr, joi.string().valid(internals.valids(fields)));
    let parts = expr.split(':');
    return new AggregateExpression(parts[0], _.find(fields, { name: parts[1] }));
};

/**
 * Returns a count expression
 * @return {AggregateExpression}
 */
AggregateExpression.count = function () {
    return new AggregateExpression(COUNT);
};

/**
 * Returns a sum expression
 * @param field
 * @return {AggregateExpression}
 */
AggregateExpression.sum = function (field) {
    return new AggregateExpression(SUM, field);
};

/**
 * Returns an average expression
 * @param field
 * @return {AggregateExpression}
 */
AggregateExpression.avg = function (field) {
    return new AggregateExpression(AVG, field);
};

/**
 * Returns a max expression
 * @param field
 * @return {AggregateExpression}
 */
AggregateExpression.max = function (field) {
    return new AggregateExpression(MAX, field);
};

/**
 * Returns a min expression
 * @param field
 * @return {AggregateExpression}
 */
AggregateExpression.min = function (field) {
    return new AggregateExpression(MIN, field);
};

/**
 * Returns a value_count expression
 * @param field
 * @return {AggregateExpression}
 */
AggregateExpression.valueCount = function (field) {
    return new AggregateExpression(VALUE_COUNT, field);
};

/**
 * Returns a cardinality expression
 * @param field
 * @return {AggregateExpression}
 */
AggregateExpression.cardinality = function (field) {
    return new AggregateExpression(CARDINALITY, field);
};

/**
 * Builds a list of all possible aggregate expressions against a collection of fields
 * @param fields a hash or array of fields
 * @param {string[] =} fns an optional array of functions to be considered (defaults to ['count', 'sum', 'avg', 'max', 'min', 'value_count', 'cardinality])
 * @return {Array.<T>}
 */
AggregateExpression.of = function (fields, fns) {
    fns = fns || [COUNT, SUM, AVG, MAX, MIN, VALUE_COUNT, CARDINALITY];

    let numberFns = _.difference(fns, [COUNT]);
    let dateFns = _.difference(fns, [COUNT, SUM, AVG]);

    let countExprs = _.intersection(fns, [COUNT]).map(() => AggregateExpression.count());

    let valueCountFns = _.difference(fns, [VALUE_COUNT]);
    let cardinalityFns = _.difference(fns, [CARDINALITY]);

    let numberExprs = _.filter(fields, { dataType: 'number' })
        .reduce(function (acc, field) {
            return acc.concat(numberFns.map(fn => new AggregateExpression(fn, field)));
        }, []);

    let dateExprs = _.filter(fields, { dataType: 'date' })
        .reduce(function (acc, field) {
            return acc.concat(dateFns.map(fn => new AggregateExpression(fn, field)));
        }, []);

    let valueCountExprs = _.reduce(fields, function (acc, field) {
        return acc.concat(valueCountFns.map(fn => new AggregateExpression(fn, field)));
    });

    return countExprs.concat(numberExprs).concat(dateExprs).concat(valueCountExprs).concat(cardinalityFns);
};

exports.AggregateExpression = AggregateExpression;