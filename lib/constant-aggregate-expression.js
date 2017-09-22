'use strict';

const _ = require('lodash');


/**
 * Represent a constant number for use an an argument to a aggregation calculation
 * @param number {number}
 * @constructor
 */
function ConstantAggregateExpression(number) {
    if(!Number.isFinite(number))
        this.number = parseFloat(number);
    else
        this.number = number;
}

/**
 * Returns a label for display in a visual
 * @returns {string}
 */
ConstantAggregateExpression.prototype.label = function () {
    return this.number;
};

/**
 * Returns a shorthand expression
 * @returns {string}
 */
ConstantAggregateExpression.prototype.toString = function() {
    return this.number;

};

/**
 * Returns an ES-friendly aggregation name (e.g. '3-14')
 * @returns {string}
 */
ConstantAggregateExpression.prototype.aggName = function() {
    return _.kebabCase(this.number);
};

/**
 * This is here to overload AggregateExpression.sort. This is not a standalone aggregate since it is a constant, and thus won't be sorted on
 * @returns {ConstantAggregateExpression}
 */
ConstantAggregateExpression.prototype.sort = function() {
    return this;
};

/**
 * Indicates that the query results need to be sorted if they are sorted by value.
 * This is not a standalone aggregate since it is a constant, and thus won't be sorted on
 * @type {boolean}
 */
ConstantAggregateExpression.prototype.needsValueSort = false;

/**
 * In other expressions, this adds a bucket aggregation to an es query, but this represents a constant, so no need
 * @returns {ConstantAggregateExpression}
 */
ConstantAggregateExpression.prototype.apply = function() {
    return this;
};

ConstantAggregateExpression.prototype.isConstant = true;

//no visit implementation, there is no need to visit. This is where the visit method would go

/**
 * return the numeric value of this constant for use in other calculations
 * @returns {number|*|Number}
 */
ConstantAggregateExpression.prototype.calculate = function() {
    return this.number;
};


exports.ConstantAggregateExpression = ConstantAggregateExpression;