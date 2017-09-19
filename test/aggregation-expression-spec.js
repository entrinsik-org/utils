'use strict';

var chai = require('chai');
var should = chai.should();
var sinon = require('sinon');
chai.use(require('sinon-chai'));
var AggregateExpression = require('../lib/aggregate-expression').AggregateExpression;
var ConstantAggregateExpression = require('../lib/constant-aggregate-expression').ConstantAggregateExpression;
var AggregateCalculationExpression = require('../lib/aggregate-calculation-expression').AggregateCalculationExpression;
var COUNT = 'count', SUM = 'sum', AVG = 'avg', MAX = 'max', MIN = 'min', VALUE_COUNT = 'value_count',
    CARDINALITY = 'cardinality';
var _ = require('lodash');

var revenue = {
    label: 'Revenue',
    id: 1,
    name: 'revenue',
    datasetId: 1,
    position: 1,
    dataType: 'double',
    typeMapping: { type: 'double' },
    isNumeric: function () {
        return true;
    }
};

var cost = {
    label: 'Cost',
    id: 2,
    name: 'cost',
    datasetId: 1,
    position: 1,
    dataType: 'double',
    typeMapping: { type: 'double' },
    isNumeric: function () {
        return true;
    }
};

var datasetFields = [revenue, cost];

describe('Aggregate Expression Parser', function () {
    it('should exist', function () {
        should.exist(AggregateExpression);
    });
    it('should parse simple expressions', function () {
        AggregateExpression.parse(datasetFields, 'count').should.deep.equal(new AggregateExpression(COUNT));
        AggregateExpression.parse(datasetFields, 'sum:cost').should.deep.equal(new AggregateExpression(SUM, cost));
        AggregateExpression.parse(datasetFields, 'avg:revenue').should.deep.equal(new AggregateExpression(AVG, revenue));
        AggregateExpression.parse(datasetFields, 'max:cost').should.deep.equal(new AggregateExpression(MAX, cost));
        AggregateExpression.parse(datasetFields, 'min:revenue').should.deep.equal(new AggregateExpression(MIN, revenue));
        AggregateExpression.parse(datasetFields, 'value_count:cost').should.deep.equal(new AggregateExpression(VALUE_COUNT, cost));
        AggregateExpression.parse(datasetFields, 'cardinality:revenue').should.deep.equal(new AggregateExpression(CARDINALITY, revenue));
    });
    it('should parse simple aggregate calculations', function () {
        var sum = new AggregateExpression(SUM, cost);
        var count = new AggregateExpression(COUNT);
        var ratio = AggregateExpression.parse(datasetFields, 'ratio(sum:cost,count)');
        should.exist(ratio);
        should.exist(ratio.fn);
        should.exist(ratio.lhs);
        should.exist(ratio.rhs);
        ratio.fn.should.equal('ratio');
        ratio.lhs.should.deep.equal(sum);
        ratio.rhs.should.deep.equal(count);
        var mult = AggregateExpression.parse(datasetFields, 'mult(sum:cost,count)');
        should.exist(mult);
        should.exist(mult.fn);
        should.exist(mult.lhs);
        should.exist(mult.rhs);
        mult.fn.should.equal('mult');
        mult.lhs.should.deep.equal(sum);
        mult.rhs.should.deep.equal(count);
        var add = AggregateExpression.parse(datasetFields, 'add(sum:cost,count)');
        should.exist(add);
        should.exist(add.fn);
        should.exist(add.lhs);
        should.exist(add.rhs);
        add.fn.should.equal('add');
        add.lhs.should.deep.equal(sum);
        add.rhs.should.deep.equal(count);
        var sub = AggregateExpression.parse(datasetFields, 'sub(sum:cost,count)');
        should.exist(sub);
        should.exist(sub.fn);
        should.exist(sub.lhs);
        should.exist(sub.rhs);
        sub.fn.should.equal('sub');
        sub.lhs.should.deep.equal(sum);
        sub.rhs.should.deep.equal(count);
    });
    it('should parse simple aggregate calculations with constants', function () {
        var max = new AggregateExpression(MAX, cost);
        var ratio = AggregateExpression.parse(datasetFields, 'ratio(max:cost,35)');
        should.exist(ratio);
        should.exist(ratio.fn);
        should.exist(ratio.lhs);
        should.exist(ratio.rhs);
        ratio.fn.should.equal('ratio');
        ratio.lhs.should.deep.equal(max);
        ratio.rhs.should.deep.equal(new ConstantAggregateExpression(35));
        var mult = AggregateExpression.parse(datasetFields, 'mult(3, max:cost)');
        should.exist(mult);
        should.exist(mult.fn);
        should.exist(mult.lhs);
        should.exist(mult.rhs);
        mult.fn.should.equal('mult');
        mult.lhs.should.deep.equal(new ConstantAggregateExpression(3));
        mult.rhs.should.deep.equal(max);
        var add = AggregateExpression.parse(datasetFields, 'add(max:cost,0.46)');
        should.exist(add);
        should.exist(add.fn);
        should.exist(add.lhs);
        should.exist(add.rhs);
        add.fn.should.equal('add');
        add.lhs.should.deep.equal(max);
        add.rhs.should.deep.equal(new ConstantAggregateExpression(0.46));
        var sub = AggregateExpression.parse(datasetFields, 'sub(1000.25, max:cost)');
        should.exist(sub);
        should.exist(sub.fn);
        should.exist(sub.lhs);
        should.exist(sub.rhs);
        sub.fn.should.equal('sub');
        sub.lhs.should.deep.equal(new ConstantAggregateExpression(1000.25));
        sub.rhs.should.deep.equal(max);
    });
    it('should parse nested aggregate calculations', function () {
        var count = AggregateExpression.parse(datasetFields,'count');
        var sum = AggregateExpression.parse(datasetFields, 'sum:revenue');
        var min = AggregateExpression.parse(datasetFields, 'min:cost');
        var ratio = AggregateExpression.parse(datasetFields, 'ratio(count,sum:revenue)');
        //mult(3,ratio(count,sum:revenue))
        var mult = AggregateExpression.parse(datasetFields, 'mult(3,ratio(count,sum:revenue))');
        should.exist(mult);
        should.exist(mult.fn);
        mult.fn.should.deep.equal('mult');
        should.exist(mult.lhs);
        mult.lhs.should.deep.equal(new ConstantAggregateExpression(3));
        should.exist(mult.rhs);
        should.exist(mult.rhs.fn);
        mult.rhs.fn.should.equal('ratio');
        should.exist(mult.rhs.lhs);
        mult.rhs.lhs.should.deep.equal(count);
        should.exist(mult.rhs.rhs);
        mult.rhs.rhs.should.deep.equal(sum);
        //ratio(sub(sum:revenue,min:cost),ratio(count,sum:revenue))
        var sub = AggregateExpression.parse(datasetFields, 'sub(sum:revenue,min:cost)');
        var complex = AggregateExpression.parse(datasetFields, 'ratio(sub(sum:revenue,min:cost),ratio(count,sum:revenue))');
        should.exist(complex);
        should.exist(complex.fn);
        complex.fn.should.deep.equal('ratio');
        //sub(sum:revenue,min:cost)
        should.exist(complex.lhs);
        should.exist(complex.lhs.fn);
        complex.lhs.fn.should.deep.equal('sub');
        should.exist(complex.lhs.lhs);
        complex.lhs.lhs.should.deep.equal(sum);
        should.exist(complex.lhs.rhs);
        complex.lhs.rhs.should.deep.equal(min);
        should.exist(complex.rhs);
        should.exist(complex.rhs.fn);
        complex.rhs.fn.should.deep.equal('ratio');
        should.exist(complex.rhs.lhs);
        complex.rhs.lhs.should.deep.equal(count);
        should.exist(complex.rhs.rhs);
        complex.rhs.rhs.should.deep.equal(sum);
        //add(ratio(count,2),ratio(count,2))
        var expr = AggregateExpression.parse(datasetFields, 'add(ratio(count,2),ratio(count,2))');
        should.exist(expr);
        should.exist(expr.fn);
        expr.fn.should.deep.equal('add');
        should.exist(expr.lhs);
        should.exist(expr.lhs.fn);
        expr.lhs.fn.should.deep.equal('ratio');
        should.exist(expr.lhs.lhs);
        expr.lhs.lhs.should.deep.equal(count);
        should.exist(expr.lhs.rhs);
        expr.lhs.rhs.should.deep.equal(new ConstantAggregateExpression(2));
        should.exist(expr.rhs);
        should.exist(expr.rhs.fn);
        expr.rhs.fn.should.deep.equal('ratio');
        should.exist(expr.rhs.lhs);
        expr.rhs.lhs.should.deep.equal(count);
        should.exist(expr.rhs.rhs);
        expr.rhs.rhs.should.deep.equal(new ConstantAggregateExpression(2));
    });
    it('should provide correct y-axis labels', function () {
        AggregateExpression.parse(datasetFields, 'add(ratio(sum:cost,avg:revenue), mult(count,4))')
            .label().should.deep.equal('(Total Cost / Average Revenue) + (Count * 4)');
        AggregateExpression.parse(datasetFields, 'mult(sub(sum:cost,avg:revenue), ratio(count,4))')
            .label().should.deep.equal('(Total Cost - Average Revenue) * (Count / 4)');
    });
    it('should provide a user-friendly error message', function() {
        var attempt = _.attempt(AggregateExpression.parse, datasetFields, 'foo');
        _.isError(attempt).should.be.true;
        attempt.message.should.deep.equal('"foo" is not a valid aggregate expression.');

    });
});