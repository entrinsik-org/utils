'use strict';

var chai = require('chai');
var should = chai.should();
var sinon = require('sinon');
chai.use(require('sinon-chai'));
var ElasticQueryTraverser = require('../lib/elastic-query-traverser').ElasticQueryTraverser;
const moment = require('moment-timezone');

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
            var EQT = new ElasticQueryTraverser(query);
            var result = EQT.traverseFilter();
            result.should.deep.equal(expectedQuery);
        });
    });

});