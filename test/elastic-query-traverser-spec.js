'use strict';

const chai = require('chai');
const should = chai.should();
const sinon = require('sinon');
chai.use(require('sinon-chai'));
const elasticQueryTransformer = require('../lib/elastic-query-transformer');
const dateKeywords = require('../lib/date-keywords').DateKeywords;
const moment = require('moment-timezone');
const _ = require('lodash');

describe('query traverser', function () {

    describe('when a query has date keywords', function () {

        it('should correctly translate the date keyword to an elastic search range filter', function () {
            var query = {
                "filter": {
                    "bool": {
                        "must": [{
                            "bool": {
                                "must": [{
                                    "range": {
                                        "OrderDate": {
                                            "date_keyword": {
                                                "value": "TODAY-30Y",
                                                "operator": "lt",
                                                "tz": "Africa/Abidjan"
                                            }
                                        }
                                    }
                                }]
                            }
                        }]
                    }
                }
            };
            var expectedQuery = {
                "filter": {
                    "bool": {
                        "must": [{
                            "bool": {
                                "must": [{
                                    "range": {
                                        "OrderDate": {
                                            lte: moment.tz(moment(), 'Africa/Abidjan').startOf('day').add(-30, 'years').toDate().getTime()
                                        }
                                    }
                                }]
                            }
                        }]
                    }
                }
            };
            const translate = elasticQueryTransformer({
                date_keyword: (query) => {
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
                    return updateQuery(query.date_keyword);
                }
            });
            const result = translate(query);
            result.should.deep.equal(expectedQuery);
        });
    });

});