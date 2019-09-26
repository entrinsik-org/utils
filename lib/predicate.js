'use strict';

const _ = require('lodash');
const Transform = require('stream').Transform;

/**
 * { $prop: { first: { $eq: 'Brad' }}}
 * @param opts
 * @return {function(*=): *}
 */
const $prop = opts => {
    const f = _(opts).keys().first();
    const p = predicate(_(opts).values().first());
    return function (v) {
        return p(_.get(v, [f]));
    };
};

/**
 * { eq: 'foo' }
 * @param opts
 * @return {function}
 */
const $eq = opts => value => _([]).concat(opts).any(v => v === value);

const $ne = opts => {
    const p = predicate(opts);
    return value => !p(value);
};

const $gt = opts => value => _([]).concat(opts).any(v => value > v);

const $gte = opts => value => _([]).concat(opts).any(v => value >= v);

const $lt = opts => value => _([]).concat(opts).any(v => value < v);

const $lte = opts => value => _([]).concat(opts).any(v => value <= v);

const $notIn = opts => $ne({ $eq: opts });

/**
 * { $between: [lo, hi] }
 * @param {boolean=} excludeEnds whether lo & hi should be accepted or rejected
 * @returns {function}
 */
const $between = excludeEnds => opts => $and([(excludeEnds ? $gt : $gte)(_.min(opts)), (excludeEnds ? $lt : $lte)(_.max(opts))]);

/**
 * { $notBetween: [lo, hi] }
 * === { $not: { $between: [lo, hi] ] }
 * === $ne($between(excludeEnds)([lo, hi]))
 * @param {boolean=} excludeEnds whether lo & hi should be accepted or rejected by the $between
 * @returns {function}
 */
const $notBetween = excludeEnds => opts => $ne($between(excludeEnds)(opts));

const $like = expr => {
    const regex = new RegExp(expr instanceof RegExp ? expr.source : expr, _.uniq(`${expr instanceof RegExp ? expr.flags : ''}`).join('').replace(/[gi]/gi, ''));
    return value => regex.test(value);
};

const $notLike = expr => $ne($like(expr));

const $ilike = expr => {
    const regex = new RegExp(expr instanceof RegExp ? expr.source : expr, _.uniq(`i${expr instanceof RegExp ? expr.flags : ''}`).join('').replace(/g/g, ''));
    return value => regex.test(value);
};

const $notIlike = expr => $ne($ilike(expr));

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
 * { and: [ { first: 'Brad' }, { last: 'Leupen' } ]
 * @param opts
 * @return {function(*=): boolean}
 */
const $and = opts => {
    const predicates = opts.map(opt => predicate(opt));
    return value => _.all(predicates, p => p(value));
};

const $or = opts => {
    const predicates = opts.map(opt => predicate(opt));
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
 * @return {function(*=): *}
 * @private
 */
function _predicate (type, opts) {
    return predicates[type] ? predicates[type](opts) : $prop({ [type]: opts });
}

function predicate (p = {}) {
    if (_.isFunction(p)) return p;
    if (!_.isPlainObject(p)) return $eq(p);

    const predicates = _.map(p, (v, k) => _predicate(k, v));

    return v => _.all(predicates, p => p(v));
}

predicate.stream = function (p = {}) {
    const test = predicate(p);
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