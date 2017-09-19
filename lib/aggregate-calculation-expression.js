'use strict';

const _ = require('lodash');
const Aggregation = require('./aggregation').Aggregation;
const defaultMapper = (label, value) => ({ label: label, value: value });

/**
 * Return null is expr is a constant, '_count' if it is an aggregate 'count' expression, or the aggName()
 * if any other aggregate expression
 * @param expr
 */
const bucketScriptExpression = function(expr) {
    if(expr.isConstant) return null;
    if(expr.fn === 'count') return '_count';
    return expr.aggName();
};


/**
 *
 * @param lhs left hand side expression
 * @param rhs right hand side expression
 * @param fn short name of function (e.g. 'ratio')
 * @param labelFn function to take the lhs and rhs and make a label for the UI  e.g (lhs, rhs) => (`${lhs} / ${rhs}`)
 * @param toStringFn function to generate toString using lhs and rhs e.g (lhs, rhs) => (`ratio(${lhs.toString()} / ${rhs.toString()})`)
 * @param aggNameFn function to generate the aggregate id in the ES query e.g (lhs, rhs) => `sub-${lhs.aggName()}-${rhs.aggName()}`
 * @param bucketScriptGenerator function to generate the bucket-script in ES e.g (a, b) => (`${a} / ${b}`)
 * @param explicitFn function to take the results of the "other" query and calculate the result, es won't do this. e.g (a, b) => (b ? a / b : null)
 * @constructor
 */
function AggregateCalculationExpression(lhs, rhs, {fn,labelFn,toStringFn,aggNameFn,bucketScriptGenerator,explicitFn}) {
    this.lhs = lhs;
    this.rhs = rhs;
    this.fn = fn;
    this.labelFunction = labelFn;
    this.toStringFunction = toStringFn;
    this.aggNameFunction = aggNameFn;
    this.bucketScriptGenerator = bucketScriptGenerator;
    this.explicitFn = explicitFn;
}

/**
 * Returns a label for display in a visual
 * @returns {string}
 */
AggregateCalculationExpression.prototype.label = function () {
    //if a side is also an AggregateCalculation, then surround with parentheses. test by looking for rhs
    var lhLabel = this.lhs.rhs ? `(${this.lhs.label()})` : this.lhs.label();
    var rhLabel = this.rhs.rhs ? `(${this.rhs.label()})` : this.rhs.label();
    return this.labelFunction(lhLabel, rhLabel);
};

/**
 * Returns a shorthand expression
 * @returns {string}
 */
AggregateCalculationExpression.prototype.toString = function() {
    return this.toStringFunction(this.lhs, this.rhs);

};

/**
 * Returns an ES-friendly aggregation name (e.g. 'ratio-sum-amount-avg-amount')
 * @returns {string}
 */
AggregateCalculationExpression.prototype.aggName = function() {
    return this.aggNameFunction(this.lhs, this.rhs);
};

/**
 * This is here to overload AggregateExpression.sort. You cannot sort on a bucket script.
 * @returns {AggregateCalculationExpression}
 */
AggregateCalculationExpression.prototype.sort = function() {
    return this;
};

/**
 * Indicates that the query results need to be sorted if they are sorted by value. The ES aggregation this uses cannot be sorted.
 * @type {boolean}
 */
AggregateCalculationExpression.prototype.needsValueSort = true;

/**
 * Applies an aggregate expression to an aggregation. Adds an ES aggregation to the query payload and configures
 * a mapper to parse the result
 * @param agg the Aggregation to add the expression to
 * @param {function=} mapper a mapping function that accepts (label, value). By default a {label: string, value: number} tuple will be used
 * @return {*}
 */
AggregateCalculationExpression.prototype.apply = function(agg, mapper) {
    mapper = mapper || defaultMapper;
    let aggId = this.aggName();
    this.lhs.apply(agg, mapper);
    this.rhs.apply(agg, mapper);
    let param1 = 'params.arg1';
    let param2 = 'params.arg2';
    let calcBody = {};

    //make a bucket script expression our of arg1. This is the reference to the other aggregation unless it's a
    //constant
    let arg1Expr = bucketScriptExpression(this.lhs);
    if(arg1Expr) {
        _.set(calcBody, 'bucket_script.buckets_path.arg1',arg1Expr);
    } else {
        //paramNumer is a constant and should be part of the bucket script
        param1 = this.lhs.toString();
    }

    let arg2Expr = bucketScriptExpression(this.rhs);
    if(arg2Expr) {
        _.set(calcBody, 'bucket_script.buckets_path.arg2', bucketScriptExpression(this.rhs));
    } else {
        param2 = this.rhs.toString();
    }

    _.set(calcBody, 'bucket_script.script', this.bucketScriptGenerator(param1,param2));
    agg.aggregation(aggId, calcBody)
        .mapper(Aggregation.bucketMapper((b, v) => mapper(b.key, v[aggId])));
    return this;
};

/**
 * Function provide to do any nesting that is necessary, like setting up the ES aggregates needed for the bucket script.
 * @param (function=} function that is called on the numerator and denominator
 */
AggregateCalculationExpression.prototype.visit = function (fn) {
    fn(this.lhs);
    fn(this.rhs);
};

/**
 * Interpret the aggregation results from ES. This is determined precisely by what results are returned from an
 *  ES query that was built in the "apply" method. This is how to get results from the bucket ('groupBy')
 *  aggregation, referring only to the terms aggregation for this expresssion. Does not include "Other" bucket (which is calculated separately.
 * @param searchResults
 * @param extractionFunction
 * @returns {*}
 */
AggregateCalculationExpression.prototype.calculate = function(searchResults, extractionFunction) {
    let arg1 = extractionFunction(this.lhs, searchResults);
    let arg2 = extractionFunction(this.rhs, searchResults);
    //return null here? undefined?
    return this.explicitFn(arg1, arg2);

};

exports.AggregateCalculationExpression = AggregateCalculationExpression;