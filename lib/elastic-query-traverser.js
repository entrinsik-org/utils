'use strict';
const _ = require('lodash');
const euDateKeywords = require('./date-keywords').DateKeywords;

function traverse (query, dateKeywords = euDateKeywords) {
    if (_.get(query, 'date_keyword') && _.get(query, 'date_keyword.tz')) {
        const translated = dateKeywords(query.date_keyword);
        const aDay = 86400000;
        const updateQuery = (info) => {
            const date = _.isDate(translated) ? translated : new Date(translated);
            const offset = date.getTime();
            switch (info.operator) {
                case 'eq':
                    return {
                        gte: offset,
                        lt: offset + aDay
                    };
                case 'gt':
                    return {
                        gt: offset + aDay - 1
                    };
                case 'gte':
                    return {
                        gte: offset
                    };
                case 'lt':
                    return {
                        lte: offset
                    };
                case 'lte':
                    return {
                        lte: offset + aDay - 1
                    };
            }
        };
        query = updateQuery(query.date_keyword);
    }
    if (_.isPlainObject(query)) {
        _.forEach(_.keys(query), (k) => {
            query[k] = traverse(query[k], dateKeywords);
        });
    }
    if (_.isArray(query)) {
        _.forEach(query, (n, i) => {
            query[i] = traverse(n, dateKeywords);
        });
    }
    return query;
}

module.exports = traverse;