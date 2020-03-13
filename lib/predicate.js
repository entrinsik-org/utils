'use strict';

const _ = require('lodash');
const Transform = require('stream').Transform;

/**
 * { $prop: { first: { $eq: 'Brad' }}}
 * @param opts
 * @param lenient
 * @return {function(*=): *}
 */
const $prop = (opts, lenient = false) => {
    const f = _(opts).keys().first();
    const p = predicate(_(opts).values().first(), lenient);
    return function (v) {
        return (lenient && !_.has(v, [f])) || p(_.get(v, [f]));
    };
};

/**
 * { eq: 'foo' }
 * @param opts
 * @return {function}
 */
const $eq = opts => value => _([]).concat(opts).any(v => _.isEqual(v, value));

const $ne = (opts, lenient) => {
    const p = predicate(opts, lenient);
    return value => !p(value);
};

const $gt = opts => value => _([]).concat(opts).any(v => value > v);

const $gte = opts => value => _([]).concat(opts).any(v => value >= v);

const $lt = opts => value => _([]).concat(opts).any(v => value < v);

const $lte = opts => value => _([]).concat(opts).any(v => value <= v);

const $notIn = (opts, lenient) => $ne({ $eq: opts }, lenient);

/**
 * { $between: [lo, hi] }
 * @param {boolean=} excludeEnds whether lo & hi should be accepted or rejected
 * @returns {function}
 */
const $between = excludeEnds => (opts, lenient) => $and([(excludeEnds ? $gt : $gte)(_.min(opts), lenient), (excludeEnds ? $lt : $lte)(_.max(opts), lenient)], lenient);

/**
 * { $notBetween: [lo, hi] }
 * === { $not: { $between: [lo, hi] ] }
 * === $ne($between(excludeEnds)([lo, hi]))
 * @param {boolean=} excludeEnds whether lo & hi should be accepted or rejected by the $between
 * @returns {function}
 */
const $notBetween = excludeEnds => (opts, lenient) => $ne($between(excludeEnds)(opts, lenient), lenient);

const $like = exprs => {
    const patterns = _.map([].concat(exprs), expr => new RegExp(expr instanceof RegExp ? expr.source : expr, _.uniq(`${expr instanceof RegExp ? expr.flags : ''}`).join('').replace(/[gi]/gi, '')));
    return value => _.any(patterns, regex => regex.test(value));
};

const $notLike = (exprs, lenient) => $ne($like(exprs, lenient), lenient);

const $ilike = exprs => {
    const patterns = _.map([].concat(exprs), expr => new RegExp(expr instanceof RegExp ? expr.source : expr, _.uniq(`i${expr instanceof RegExp ? expr.flags : ''}`).join('').replace(/g/g, '')));
    return value => _.any(patterns, regex => regex.test(value));
};

const $notIlike = (exprs, lenient) => $ne($ilike(exprs, lenient), lenient);

/**
 * Evaluates to true IFF ALL provided opts exist in the
 * value (ignoring order), according to rules of _.contains:
 *
 * if tested value is an array, checks if members match
 * if tested value is an object, checks if values match
 * if tested value is a string, checks contents for match
 * if tested value is not a collection, it is coerced to a collection
 *
 * { $contains: 'foo' } === _.contains(value, 'foo')
 * { $contains: [2, 1] === _.all([2, 1], v => _.contains(value, v))
 *
 * @param opts
 * @returns {function}
 */
const $contains = opts => value => {
    const testVal = (_.isArray(value) || _.isPlainObject(value) || _.isString(value)) ? value : [value];
    return _([]).concat(opts).uniq().all(v => _.contains(testVal, v));
};

/**
 * { and: [{ first: 'Brad' }, { last: 'Leupen' }] }
 * @param opts
 * @param lenient
 * @return {function(*=): boolean}
 */
const $and = (opts, lenient) => {
    const predicates = opts.map(opt => predicate(opt, lenient));
    return value => _.all(predicates, p => p(value));
};

/**
 * { or: [{ first: 'Brad' }, { last: 'Leupen' }] }
 * @param opts
 * @param lenient
 * @returns {function(*=): boolean}
 */
const $or = (opts, lenient) => {
    const predicates = opts.map(opt => predicate(opt, lenient));
    return value => _.any(predicates, p => p(value));
};

const $true = () => true;

const predicates = {
    $eq,
    $ne,
    $neq: $ne,
    $and,
    $or,
    $gt,
    $gte,
    $lt,
    $lte,
    $prop,
    $not: $ne,
    $in: $eq,
    $notIn,
    $between: $between(false),
    $notBetween: $notBetween(false),
    $betweenInclusive: $between(false),
    $betweenExclusive: $between(true),
    $like: $like,
    $notLike: $notLike,
    $ilike: $ilike,
    $notIlike: $notIlike,
    $contains: $contains
};

/**
 * Compiles a single predicate
 * @param type
 * @param opts
 * @param lenient
 * @return {function(*=): *}
 * @private
 */
function _predicate (type, opts, lenient) {
    return predicates[type] ? predicates[type](opts, lenient) : $prop({ [type]: opts }, lenient);
}

function predicate (p = {}, lenient = false) {
    if (_.isFunction(p)) return p;
    if (!_.isPlainObject(p)) return $eq(p);

    const predicates = _.map(p, (v, k) => _predicate(k, v, lenient));

    return v => _.all(predicates, p => p(v));
}

predicate.stream = function (p = {}, lenient = false) {
    const test = predicate(p, lenient);
    return new Transform({
        objectMode: true,
        transform (rec, enc, next) {
            if (test(rec)) {
                next(null, rec);
            } else {
                next();
            }
        }
    })
};

module.exports = predicate;