'use strict';
const _ = require('lodash');

function traverse (keys, query) {
    if (_.isPlainObject(query)) {
        _.forEach(_.keys(query), (k) => {
            if(_.isFunction(keys[k])){
                query = keys[k](query);
            }
            else {
                query[k] = traverse(keys, query[k]);
            }
        });
    }
    if (_.isArray(query)) {
        _.forEach(query, (n, i) => {
            if(_.isFunction(keys[i])){
                query = keys[i](query);
            }
            else {
                query[i] = traverse(keys, n);
            }
        });
    }
    return query;
}

module.exports = _.curry(traverse);