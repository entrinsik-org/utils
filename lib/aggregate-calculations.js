/**
 * Created by andrewmorovati on 8/1/17.
 */
'use strict';
const _ = require('lodash');
const joi = require('joi');
const ConstantAggregateExpression = require('./constant-aggregate-expression').ConstantAggregateExpression;
const AggregateCalculationExpression = require('./aggregate-calculation-expression').AggregateCalculationExpression;
const AggregateExpression = require('./aggregate-expression');

const internals = {};
const parsers = {};


/**
 * take a potentially nested function expression and return the top parts. Enables functions to be nested
 * @param expr
 * @returns {Array}
 */
internals.splitFunction = function (expr) {
    let parts = [];
    let pDpth = 0;
    let i = 0;
    expr.split('').forEach(function (c) {
        if (c === '(') pDpth++;
        if (c === ')') pDpth--;
        if (c === ',' && pDpth === 0) i++;
        else {
            parts[i] = parts[i] || '';
            parts[i] += c;
        }
    });
    return parts;
};

/**
 * Does the boilerplate work of parsing the expression arguments and calling the constructor
 * @param fields
 * @param expr
 * @param config
 * @returns {AggregateCalculationExpression}
 */
internals.parse = function (fields, expr, config) {
    //validate on the regex, a validation error is swallowed in the caller
    joi.assert(expr, joi.string().regex(config.pattern));
    //extract the arguments from the expr
    const blob = config.pattern.exec(expr)[1];
    //split on the first comma not in a parentheses
    const args = internals.splitFunction(blob);
    const arg1 = AggregateExpression.AggregateExpression.parse(fields, args[0].trim());
    const arg2 = AggregateExpression.AggregateExpression.parse(fields, args[1].trim());
    return new AggregateCalculationExpression(arg1, arg2, config);


};
/**
 * parse an expression 'ratio(exp1,exp2)' into a calculation expression that divides the first arg by the second
 * @type {{parse: parsers.ratio.parse}}
 */
parsers.ratio = {
    parse: function (fields, expr) {
        const config = {
            pattern: /^ratio\(([^]+)\)/,
            fn: 'ratio',
            labelFn: (lhs, rhs) => (`${lhs} / ${rhs}`),
            toStringFn: (lhs, rhs) => (`ratio(${lhs.toString()} / ${rhs.toString()})`),
            aggNameFn: (lhs, rhs) => `ratio-${lhs.aggName()}-${rhs.aggName()}`,
            bucketScriptGenerator: (a, b) => (`${a} / ${b}`),
            explicitFn: (a, b) => ( b ? a / b : null)
        };
        return internals.parse(fields, expr, config);

    }
};

/**
 * parse an expression 'mult(exp1,exp2)' into a calculation expression that multiplies the 2 args
 * @type {{parse: parsers.mult.parse}}
 */
parsers.mult = {
    parse: function (fields, expr) {
        const config = {
            pattern: /^mult\(([^]+)\)/,
            fn: 'mult',
            labelFn: (lhs, rhs) => (`${lhs} * ${rhs}`),
            toStringFn: (lhs, rhs) => (`mult(${lhs.toString()} * ${rhs.toString()})`),
            aggNameFn: (lhs, rhs) => `mult-${lhs.aggName()}-${rhs.aggName()}`,
            bucketScriptGenerator: (a, b) => (`${a} * ${b}`),
            explicitFn: (a, b) => ( a * b)
        };
        return internals.parse(fields, expr, config);
    }
};

/**
 * parse an expression 'add(exp1,exp2)' into a calculation expression that adds the 2 args
 * @type {{parse: parsers.add.parse}}
 */
parsers.add = {
    parse: function (fields, expr) {
        const config = {
            pattern: /^add\(([^]+)\)/,
            fn: 'add',
            labelFn: (lhs, rhs) => (`${lhs} + ${rhs}`),
            toStringFn: (lhs, rhs) => (`add(${lhs.toString()} + ${rhs.toString()})`),
            aggNameFn: (lhs, rhs) => `add-${lhs.aggName()}-${rhs.aggName()}`,
            bucketScriptGenerator: (a, b) => (`${a} + ${b}`),
            explicitFn: (a, b) => ( a + b)
        };
        return internals.parse(fields, expr, config);
    }
};

/**
 * parse an expression 'sub(exp1,exp2)' into a calculation expression that subtracts exp2 from exp1
 * @type {{parse: parsers.sub.parse}}
 */
parsers.sub = {
    parse: function (fields, expr) {
        const config = {
            pattern: /^sub\(([^]+)\)/,
            fn: 'sub',
            labelFn: (lhs, rhs) => (`${lhs} - ${rhs}`),
            toStringFn: (lhs, rhs) => (`sub(${lhs.toString()} - ${rhs.toString()})`),
            aggNameFn: (lhs, rhs) => `sub-${lhs.aggName()}-${rhs.aggName()}`,
            bucketScriptGenerator: (a, b) => (`${a} - ${b}`),
            explicitFn: (a, b) => ( a - b)
        };
        return internals.parse(fields, expr, config);
    }
};

/**
 * parse an numeric expression ('1', '0.23', '23.22') into a ConstantAggregateExpression
 * @type {{parse: parsers.constant.parse}}
 */
parsers.constant = {
    parse: function (fields, expr) {
        joi.assert(expr, joi.string().regex(/^-?\d+\.?\d*$/));
        return new ConstantAggregateExpression(expr);
    }
};


/**
 * Attempt to parse all the expression types, stopping at the first that works
 * @param fields
 * @param expr
 */
exports.parseExpression = function (fields, expr) {
    return _.values(parsers).reduce(function (match, element) {
        if (!match) {
            const attempt = _.attempt(element.parse, fields, expr);
            if (!_.isError(attempt)) {
                return attempt;
            }
        }
        return match;
    }, null);
};



