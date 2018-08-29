'use strict';
const _ = require('lodash');
const euDateKeywords = require('./date-keywords').DateKeywords;

function ElasticQueryTraverser (query, dateKeywords = euDateKeywords){
    this.query = query;
    this.dateKeywords = dateKeywords;
}

ElasticQueryTraverser.prototype.traverseFilter = function (){
    return this.traverse(this.query);
};

ElasticQueryTraverser.prototype.traverse = function (query){
    if (_.get(query, 'date_keyword') && _.get(query, 'date_keyword.tz')) {
        const translated = this.dateKeywords(query.date_keyword);
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
            query[k] = this.traverse(query[k]);
        });
    }
    if (_.isArray(query)) {
        _.forEach(query, (n, i) => {
            query[i] = this.traverse(n);
        });
    }
    return query;
};

exports.ElasticQueryTraverser = ElasticQueryTraverser;