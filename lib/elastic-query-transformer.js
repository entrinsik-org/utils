'use strict';
const _ = require('lodash');

function traverse (query, keys) {
    if (_.isPlainObject(query)) {
        _.forEach(_.keys(query), (k) => {
            if(_.isFunction(keys[k])){
                query = keys[k](query);
            }
            else {
                query[k] = traverse(query[k], keys);
            }
        });
    }
    if (_.isArray(query)) {
        _.forEach(query, (n, i) => {
            if(_.isFunction(keys[i])){
                query = keys[i](query);
            }
            else {
                query[i] = traverse(n, keys);
            }
        });
    }
    return query;
}

module.exports = traverse;