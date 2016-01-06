'use strict';

var chai = require('chai');
var should = chai.should();
var sinon = require('sinon');
chai.use(require('sinon-chai'));
var Aggregation = require('../lib/aggregation').Aggregation;

describe('AggBuilder', function () {
    it('should exist', function () {
        should.exist(Aggregation);
    });

    it('should support an explicit body', function () {
        var agg = new Aggregation()
            .aggregation('my_agg', { terms: { foo: 'bar' } })
            .build();

        agg.should.deep.equal({
            aggregations: {
                my_agg: {
                    terms: {
                        foo: 'bar'
                    }
                }
            }
        });
    });

    it('should support callback builder', function () {
        var spy = sinon.spy();

        new Aggregation().aggregation('my_agg', { terms: { field: 'foo' } }, spy);

        spy.should.have.been.called;
    });

    it('should support a term with a nested sum', function () {
        var agg = new Aggregation()
            .aggregation('states', Aggregation.terms('state'), function (agg) {
                agg.aggregation('total_amount', Aggregation.sum('amount'));
            })
            .build();

        agg.should.deep.equal({
            aggregations: {
                states: {
                    terms: {
                        field: 'state'
                    },
                    aggregations: {
                        total_amount: {
                            sum: { field: 'amount' }
                        }
                    }
                }
            }
        });
    });

    it('should support setting mapper functions', function () {
        var agg = new Aggregation()
            .aggregation('states', Aggregation.terms('state'), function (agg) {
                agg.mapper(function (result) {
                    return result.buckets.reduce(function (acc, bucket) {
                        acc[bucket.key] = bucket.doc_count;
                        return acc;
                    }, {});
                });
            });

        var data = require('./states.json');
        var res = agg.map(data);
        res.should.deep.equal({
            states: {
                CA: 31,
                IL: 16,
                MA: 15,
                NC: 67,
                NJ: 22,
                NY: 21,
                OH: 16,
                PA: 21,
                SC: 11,
                TX: 16
            }
        });
    });

    it('should provide a sane default mapper for terms', function () {
        var agg = new Aggregation().aggregation('states', Aggregation.terms('state'));

        var data = require('./states.json');
        var res = agg.map(data);
        res.should.deep.equal({
            states: {
                CA: { count: 31 },
                IL: { count: 16 },
                MA: { count: 15 },
                NC: { count: 67 },
                NJ: { count: 22 },
                NY: { count: 21 },
                OH: { count: 16 },
                PA: { count: 21 },
                SC: { count: 11 },
                TX: { count: 16 }
            }
        });
    });

    it('should support nested mappers', function () {
        var agg = new Aggregation()
            .aggregation('states', Aggregation.terms('state'), function (agg) {
                agg.aggregation('total_amount', Aggregation.sum('amount'), function (agg) {
                    agg.mapper(function (value) {
                        return value.value;
                    });
                });
            });
        var data = require('./state_sum_amount.json');
        var res = agg.map(data);
        res.should.deep.equal({
            states: {
                CA: { count: 31, total_amount: 421287.5 },
                IL: { count: 16, total_amount: 391640 },
                MA: { count: 15, total_amount: 245480 },
                NC: { count: 67, total_amount: 1165652 },
                NJ: { count: 22, total_amount: 399075 },
                NY: { count: 21, total_amount: 419935 },
                OH: { count: 16, total_amount: 340320 },
                PA: { count: 21, total_amount: 299419 },
                SC: { count: 11, total_amount: 156672 },
                TX: { count: 16, total_amount: 344090 }
            }
        });
    });

    it('should support new api', function () {
        var agg = new Aggregation()
            .agg('states', Aggregation.terms('state'), function (agg) {
                agg.agg('total_amount', Aggregation.sum('amount'));
                agg.agg('avg_amount', Aggregation.avg('amount'));
                agg.agg('max_amount', Aggregation.max('amount'));
                agg.agg('min_amount', Aggregation.min('amount'));
            });

        agg.build().should.deep.equal({
            aggregations: {
                states: {
                    terms: {
                        field: 'state'
                    },
                    aggregations: {
                        avg_amount: {
                            avg: { field: 'amount' }
                        },
                        max_amount: {
                            max: { field: 'amount' }
                        },
                        min_amount: {
                            min: { field: 'amount' }
                        },
                        total_amount: {
                            sum: { field: 'amount' }
                        }
                    }
                }
            }
        });

        var res = agg.map(require('./state_sum_amount.json'));

        res.should.deep.equal({
            "states": {
                "CA": {
                    "avg_amount": 13589.91935483871,
                    "count": 31,
                    "max_amount": 30000,
                    "min_amount": 3500,
                    "total_amount": 421287.5
                },
                "IL": {
                    "avg_amount": 24477.5,
                    "count": 16,
                    "max_amount": 70400,
                    "min_amount": 8750,
                    "total_amount": 391640
                },
                "MA": {
                    "avg_amount": 16365.333333333334,
                    "count": 15,
                    "max_amount": 44000,
                    "min_amount": 4500,
                    "total_amount": 245480
                },
                "NC": {
                    "avg_amount": 17397.79104477612,
                    "count": 67,
                    "max_amount": 73000,
                    "min_amount": 1,
                    "total_amount": 1165652
                },
                "NJ": {
                    "avg_amount": 18139.772727272728,
                    "count": 22,
                    "max_amount": 45000,
                    "min_amount": 2500,
                    "total_amount": 399075
                },
                "NY": {
                    "avg_amount": 19996.904761904763,
                    "count": 21,
                    "max_amount": 52800,
                    "min_amount": 6500,
                    "total_amount": 419935
                },
                "OH": {
                    "avg_amount": 21270,
                    "count": 16,
                    "max_amount": 66560,
                    "min_amount": 4500,
                    "total_amount": 340320
                },
                "PA": {
                    "avg_amount": 14258.047619047618,
                    "count": 21,
                    "max_amount": 25000,
                    "min_amount": 3500,
                    "total_amount": 299419
                },
                "SC": {
                    "avg_amount": 14242.90909090909,
                    "count": 11,
                    "max_amount": 28072,
                    "min_amount": 4350,
                    "total_amount": 156672
                },
                "TX": {
                    "avg_amount": 21505.625,
                    "count": 16,
                    "max_amount": 58080,
                    "min_amount": 5000,
                    "total_amount": 344090
                }
            }
        });
    });

    it('should support stats', function () {
        var agg = new Aggregation()
            .aggregation('amount_stats', Aggregation.stats('amount'));

        agg.build().should.deep.equal({
            "aggregations": {
                "amount_stats": {
                    "stats": {
                        "field": "amount"
                    }
                }
            }
        });
        agg.map(require('./amount_stats.json')).should.deep.equal({
            amount_stats: {
                "count": 504,
                "min": 0,
                "max": 107360,
                "avg": 18098.435515873014,
                "sum": 9121611.5
            }
        });
    });

    it('should support value_count', function () {
        var agg = new Aggregation()
            .aggregation('state_count', Aggregation.valueCount('state'));

        agg.build().should.deep.equal({
            "aggregations": {
                "state_count": {
                    "value_count": {
                        "field": "state"
                    }
                }
            }
        });

        agg.map(require('./state_value_count.json')).should.deep.equal({
            "state_count": 378
        });
    });

    it('should support extended_stats', function () {
        var agg = new Aggregation()
            .aggregation('amount_stats', Aggregation.extendedStats('amount'));

        agg.build().should.deep.equal({
            "aggregations": {
                "amount_stats": {
                    "extended_stats": {
                        "field": "amount"
                    }
                }
            }
        });

        agg.map(require('./amount_extended_stats.json')).should.deep.equal({
            "amount_stats": {
                "avg": 18098.435515873014,
                "count": 504,
                "max": 107360,
                "min": 0,
                "std_deviation": 12607.430829331466,
                "std_deviation_bounds": {
                    "lower": -7116.426142789918,
                    "upper": 43313.29717453595
                },
                "sum": 9121611.5,
                "sum_of_squares": 245196342840.25,
                "variance": 158947312.1163775
            }
        });
    });

    it('should configure the aggregation', function () {
        var agg = new Aggregation()
            .aggregation('states', Aggregation.terms('state'), function (agg) {
                agg.configure({ sort: { field: 'asc' } });
            })
            .build();

        agg.should.deep.equal({
            "aggregations": {
                "states": {
                    "terms": {
                        "field": "state",
                        "sort": { "field": "asc" }
                    }
                }
            }
        });
    });
});