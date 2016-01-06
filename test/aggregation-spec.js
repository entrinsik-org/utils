'use strict';

var should = require('chai').should();
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
        var agg = new Aggregation()
            .aggregation('my_agg', function (agg) {
                agg.terms({ foo: 'bar' });
            })
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

    it('should support a term with a nested sum', function () {
        var agg = new Aggregation()
            .aggregation('states', function (agg) {
                agg.terms({ field: 'state' });
                agg.aggregation('total_amount', function (agg) {
                    agg.sum({ field: 'amount' });
                })
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
            .aggregation('states', function (agg) {
                agg.terms({ field: 'state' })
                    .mapper(function (result) {
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
        var agg = new Aggregation()
            .aggregation('states', function (agg) {
                agg.terms({ field: 'state' });
            });

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
            .aggregation('states', function (agg) {
                agg.terms({ field: 'state' })
                    .aggregation('total_amount', function (agg) {
                        agg.sum({ field: 'amount' })
                            .mapper(function (value) {
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
});