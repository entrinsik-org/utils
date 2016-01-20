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

    it('should support a bucket reducer mapper', function () {
        new Aggregation()
            .aggregation('states', Aggregation.terms('state'), function (agg) {
                agg
                    .aggregation('total_amount', Aggregation.sum('amount'))
                    .mapper(Aggregation.bucketReducer(function (bucket, value) {
                        return [bucket.key, bucket.doc_count, value.total_amount];
                    }));
            })
            .map(require('./state_sum_amount.json')).should.deep.equal({
            "states": {
                "CA": [
                    "CA",
                    31,
                    421287.5
                ],
                "IL": [
                    "IL",
                    16,
                    391640
                ],
                "MA": [
                    "MA",
                    15,
                    245480
                ],
                "NC": [
                    "NC",
                    67,
                    1165652
                ],
                "NJ": [
                    "NJ",
                    22,
                    399075
                ],
                "NY": [
                    "NY",
                    21,
                    419935
                ],
                "OH": [
                    "OH",
                    16,
                    340320
                ],
                "PA": [
                    "PA",
                    21,
                    299419
                ],
                "SC": [
                    "SC",
                    11,
                    156672
                ],
                "TX": [
                    "TX",
                    16,
                    344090
                ]
            }
        });
    });

    it('should support a bucket mapper mapper', function () {
        new Aggregation()
            .aggregation('states', Aggregation.terms('state'), function (agg) {
                agg
                    .aggregation('total_amount', Aggregation.sum('amount'))
                    .mapper(Aggregation.bucketMapper(function (bucket, value) {
                        return [bucket.key, bucket.doc_count, value.total_amount];
                    }));
            })
            .map(require('./state_sum_amount.json')).should.deep.equal({
            "states": [
                [
                    "NC",
                    67,
                    1165652
                ],
                [
                    "CA",
                    31,
                    421287.5
                ],
                [
                    "NJ",
                    22,
                    399075
                ],
                [
                    "NY",
                    21,
                    419935
                ],
                [
                    "PA",
                    21,
                    299419
                ],
                [
                    "IL",
                    16,
                    391640
                ],
                [
                    "OH",
                    16,
                    340320
                ],
                [
                    "TX",
                    16,
                    344090
                ],
                [
                    "MA",
                    15,
                    245480
                ],
                [
                    "SC",
                    11,
                    156672
                ]
            ]
        });
    });

    it('should support a histogram', function () {
        new Aggregation()
            .aggregation('my_histogram', Aggregation.histogram({ field: 'amount', interval: 10000}))
            .build()
            .should.deep.equal({
            "aggregations": {
                "my_histogram": {
                    "histogram": {
                        "field": "amount",
                        "interval": 10000
                    }
                }
            }
        });
    });

    it('should parse histogram data', function () {
        new Aggregation()
            .aggregation('my_histogram', Aggregation.histogram({ field: 'amount', interval: 10000}))
            .map(require('./histogram_counts.json'))
            .should.deep.equal({
            "my_histogram": [
                {
                    "count": 115,
                    "value": 0
                },
                {
                    "count": 225,
                    "value": 10000
                },
                {
                    "count": 109,
                    "value": 20000
                },
                {
                    "count": 25,
                    "value": 30000
                },
                {
                    "count": 10,
                    "value": 40000
                },
                {
                    "count": 10,
                    "value": 50000
                },
                {
                    "count": 5,
                    "value": 60000
                },
                {
                    "count": 4,
                    "value": 70000
                },
                {
                    "count": 1,
                    "value": 100000
                }
            ]
        });
    });

    it('should support a date histogram', function () {
        new Aggregation()
            .aggregation('counts_over_time', Aggregation.dateHistogram('date_closed'))
            .build()
            .should.deep.equal({
                "aggregations": {
                    "counts_over_time": {
                        "date_histogram": {
                            "field": "date_closed",
                            "interval": "month"
                        }
                    }
                }
            });
    });

    it('should parse histogram data', function () {
        new Aggregation()
            .aggregation('counts_over_time', Aggregation.dateHistogram({ field: 'date_closed', interval: 'year'}), function(agg) {
                agg.aggregation('total_amount', Aggregation.sum('amount'))
            })
            .map(require('./date_histogram_counts.json'))
            .should.deep.equal({
            "counts_over_time": [
                {
                    "count": 1,
                    "date": "2003-01-01T00:00:00.000Z",
                    "total_amount": 17500
                },
                {
                    "count": 47,
                    "date": "2008-01-01T00:00:00.000Z",
                    "total_amount": 831670
                },
                {
                    "count": 17,
                    "date": "2009-01-01T00:00:00.000Z",
                    "total_amount": 279430
                },
                {
                    "count": 36,
                    "date": "2010-01-01T00:00:00.000Z",
                    "total_amount": 658982
                },
                {
                    "count": 75,
                    "date": "2011-01-01T00:00:00.000Z",
                    "total_amount": 1368760
                },
                {
                    "count": 118,
                    "date": "2012-01-01T00:00:00.000Z",
                    "total_amount": 1783493.5
                },
                {
                    "count": 60,
                    "date": "2013-01-01T00:00:00.000Z",
                    "total_amount": 1251663
                },
                {
                    "count": 100,
                    "date": "2014-01-01T00:00:00.000Z",
                    "total_amount": 1942801
                },
                {
                    "count": 46,
                    "date": "2015-01-01T00:00:00.000Z",
                    "total_amount": 872370
                },
                {
                    "count": 3,
                    "date": "2016-01-01T00:00:00.000Z",
                    "total_amount": 47942
                },
                {
                    "count": 1,
                    "date": "2017-01-01T00:00:00.000Z",
                    "total_amount": 67000
                }
            ]
        });
    });

    it('should support a geohash grid', function () {
        new Aggregation()
            .aggregation('geohash_grid', Aggregation.geohashGrid('location'))
            .build()
            .should.deep.equal({
            "aggregations": {
                "geohash_grid": {
                    "geohash_grid": {
                        "field": "location"
                    }
                }
            }
        });
    });

    it('should parse geohash grid data', function () {
        new Aggregation()
            .aggregation('geohash_grid', Aggregation.geohashGrid({ field: 'location', precision: 2 }))
            .map(require('./geohash_grid.json'))
            .should.deep.equal({
            "geohash_grid": {
                "10": {
                    "count": 7
                },
                "11": {
                    "count": 8
                },
                "15": {
                    "count": 8
                },
                "27": {
                    "count": 8
                },
                "36": {
                    "count": 16
                },
                "37": {
                    "count": 1
                },
                "08": {
                    "count": 166
                },
                "09": {
                    "count": 246
                },
                "0b": {
                    "count": 183
                },
                "0c": {
                    "count": 68
                },
                "0d": {
                    "count": 13
                },
                "0e": {
                    "count": 67
                },
                "0f": {
                    "count": 133
                },
                "0g": {
                    "count": 68
                },
                "0s": {
                    "count": 91
                },
                "0t": {
                    "count": 1
                },
                "0u": {
                    "count": 89
                },
                "0v": {
                    "count": 378
                },
                "0y": {
                    "count": 6
                },
                "1h": {
                    "count": 11
                },
                "1j": {
                    "count": 137
                },
                "1n": {
                    "count": 3
                },
                "1r": {
                    "count": 1
                },
                "2k": {
                    "count": 18
                },
                "3m": {
                    "count": 1
                },
                "9m": {
                    "count": 5
                },
                "9q": {
                    "count": 29
                },
                "9r": {
                    "count": 6
                },
                "9t": {
                    "count": 1
                },
                "9u": {
                    "count": 1
                },
                "9v": {
                    "count": 15
                },
                "9w": {
                    "count": 1
                },
                "9x": {
                    "count": 7
                },
                "9y": {
                    "count": 14
                },
                "9z": {
                    "count": 13
                },
                "c2": {
                    "count": 5
                },
                "dh": {
                    "count": 3
                },
                "dj": {
                    "count": 11
                },
                "dn": {
                    "count": 61
                },
                "dp": {
                    "count": 50
                },
                "dq": {
                    "count": 42
                },
                "dr": {
                    "count": 37
                },
                "h8": {
                    "count": 174
                },
                "h9": {
                    "count": 842
                },
                "hb": {
                    "count": 683
                },
                "hc": {
                    "count": 5088
                },
                "hf": {
                    "count": 18439
                },
                "hg": {
                    "count": 13
                },
                "j0": {
                    "count": 5
                }
            }
        });
    });
});