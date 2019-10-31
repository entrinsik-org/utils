'use strict';

const p = require('../lib/predicate');
const stream = require('stream');
const _ = require('lodash');

describe.only('predicate', function () {
    function dataStream (data) {
        return new stream.Readable({
            objectMode: true,
            read () {
                this.push(data.shift() || null);
            }
        });
    }

    it(`should handle an empty object`, () => {
        const test = p({});
        test('Brad').should.be.true;
    });

    it(`should handle undefined`, () => {
        const test = p();
        test('Brad').should.be.true;
    });

    describe('$eq', function () {
        it('should compare object equality', function () {
            const test = p({ $eq: 'Brad' });

            test('Brad').should.be.true;
            test('Hank').should.be.false;
        });

        it(`should handle an array of values as a disjunction [x OR y ...]`, () => {
            const test = p({ $eq: ['Brad', 'Hank'] });
            test('Brad').should.be.true;
            test('Hank').should.be.true;
        });
    });

    describe('$ne', function () {
        it('should compare object equality', function () {
            const test = p({ $ne: 'Brad' });
            test('Brad').should.be.false;
            test('Hank').should.be.true;
        });

        it(`should handle an array of values as a conjunction (NOT [x OR y ...])`, () => {
            const test = p({ $ne: ['Brad', 'Hank'] });
            test('Brad').should.be.false;
            test('Hank').should.be.false;
            test('Foo').should.be.true;
        });
    });

    describe('$prop', function () {
        it('should test long hand property $eq', function () {
            const test = p({ $prop: { first: { $eq: 'Brad' } } });
            test({ first: 'Brad' }).should.be.true;
            test({ first: 'Hank' }).should.be.false;
        });

        it('should test medium hand property $eq', function () {
            const test = p({ $prop: { first: 'Brad' } });
            test({ first: 'Brad' }).should.be.true;
            test({ first: 'Hank' }).should.be.false;
        });

        it('should test short hand single property $eq', function () {
            const test = p({ first: 'Brad' });
            test({ first: 'Brad' }).should.be.true;
            test({ first: 'Hank' }).should.be.false;
        });

        it(`should test backhand property neq`, () => {
            const test = p({ first: { $ne: 'Brad' } });
            test({ first: 'Brad' }).should.be.false;
            test({ first: 'Hank' }).should.be._true;
        });

        it('should perform an IN test against an array of values', function () {
            const test = p({ first: ['Brad', 'Hank'] });
            test({ first: 'Brad' }).should.be.true;
            test({ first: 'Hank' }).should.be.true;
        });

        it('should conjoin tests for multiple properties', function () {
            const test = p({ first: 'Brad', last: 'Leupen' });
            test({ first: 'Brad', last: 'Leupen' }).should.be.true;
            test({ first: 'Hank', last: 'Leupen' }).should.be.false;
            test({ first: 'Brad', last: 'Pitt' }).should.be.false;
        });

        it('should combine $eq with $neq', function () {
            const test = p({ first: { $ne: 'Brad' }, last: 'Leupen' });
            test({ first: 'Brad', last: 'Leupen' }).should.be.false;
            test({ first: 'Hank', last: 'Leupen' }).should.be.true;
        });

        it(`should allow leniency to pass the test of an object when the object does not have the property but still perform normally when the property exists`, () => {
            // shorthand
            const shTest = p({ first: 'Brad' }, true);
            shTest({ foo: 'bar' }).should.be.true;
            shTest({ foo: 'bar', first: 'Brad' }).should.be.true;
            shTest({ foo: 'bar', first: 'Hank' }).should.be.false;
            // mediumhand
            const mhTest = p({ $prop: { first: 'Brad' } }, true);
            mhTest({ foo: 'bar' }).should.be.true;
            mhTest({ foo: 'bar', first: 'Brad' }).should.be.true;
            mhTest({ foo: 'bar', first: 'Hank' }).should.be.false;
            // longhand
            const lhTest = p({ $prop: { first: { $eq: 'Brad' } } }, true);
            lhTest({ foo: 'bar' }).should.be.true;
            lhTest({ foo: 'bar', first: 'Brad' }).should.be.true;
            lhTest({ foo: 'bar', first: 'Hank' }).should.be.false;
            // backhand neq
            const bhTest = p({ first: { $ne: 'Brad' } }, true);
            bhTest({ foo: 'bar' }).should.be.true;
            bhTest({ foo: 'bar', first: 'Brad' }).should.be.false;
            bhTest({ foo: 'bar', first: 'Hank' }).should.be.true;
            // IN
            const inTest = p({ first: ['Brad', 'Hank'] }, true);
            inTest({ foo: 'bar' }).should.be.true;
            inTest({ foo: 'bar', first: 'Brad' }).should.be.true;
            inTest({ foo: 'bar', first: 'Hank' }).should.be.true;
            inTest({ foo: 'bar', first: 'Foo' }).should.be.false;
            // conjoined
            const conTest = p({ first: 'Brad', last: 'Leupen' }, true);
            conTest({ foo: 'bar' }).should.be.true;
            conTest({ foo: 'bar', first: 'Brad' }).should.be.true;
            conTest({ foo: 'bar', first: 'Hank' }).should.be.false;
            conTest({ foo: 'bar', last: 'Leupen' }).should.be.true;
            conTest({ foo: 'bar', last: 'Pitt' }).should.be.false;
            conTest({ foo: 'bar', first: 'Brad', last: 'Leupen' }).should.be.true;
            conTest({ foo: 'bar', first: 'Hank', last: 'Leupen' }).should.be.false;
            conTest({ foo: 'bar', first: 'Brad', last: 'Pitt' }).should.be.false;
        });
    });

    describe('$or', function () {
        it('should or multiple property tests', function () {
            const test = p({ $or: [{ first: 'Brad' }, { last: 'Leupen' }] });
            test({ first: 'Brad', last: 'Leupen' }).should.be.true;
            test({ first: 'Hank', last: 'Leupen' }).should.be.true;
            test({ first: 'Brad', last: 'Pitt' }).should.be.true;
            test({ first: 'Mitt', last: 'Pitt' }).should.be.false;
        });
    });

    describe('$gt', function () {
        it('should test numbers', function () {
            const test = p({ $gt: 40 });
            test(41).should.be.true;
            test(40).should.be.false;
            test(39).should.be.false;
        });

        it('should test dates', function () {
            const d1 = new Date('2016-01-01');
            const d2 = new Date('2017-01-01');
            const d3 = new Date('2018-01-01');

            const test = p({ $gt: d2 });
            test(d1).should.be.false;
            test(d2).should.be.false;
            test(d3).should.be.true;
        });

        it(`should handle an array of 1 value as if it were singular`, () => {
            const test = p({ $gt: [40] });
            test(41).should.be.true;
            test(40).should.be.false;
            test(39).should.be.false;
            test(30).should.be.false;
        });

        it(`should handle an array of values for the predicate as a disjunction [x gt N OR x gt M ...]`, () => {
            const test = p({ $gt: [40, 30] });
            test(41).should.be.true;
            test(40).should.be.true;
            test(39).should.be.true;
            test(30).should.be.false;
            test(29).should.be.false;
        });
    });

    describe('$gte', function () {
        it('should test numbers', function () {
            const test = p({ $gte: 40 });
            test(41).should.be.true;
            test(40).should.be.true;
            test(39).should.be.false;
        });

        it('should test dates', function () {
            const d1 = new Date('2016-01-01');
            const d2 = new Date('2017-01-01');
            const d3 = new Date('2018-01-01');

            const test = p({ $gte: d2 });
            test(d1).should.be.false;
            test(d2).should.be.true;
            test(d3).should.be.true;
        });

        it(`should handle an array of 1 value as if it were singular`, () => {
            const test = p({ $gte: [40] });
            test(41).should.be.true;
            test(40).should.be.true;
            test(39).should.be.false;
            test(30).should.be.false;
        });

        it(`should handle an array of values for the predicate as a disjunction [x gte N OR x gte M ...]`, () => {
            const test = p({ $gte: [40, 30] });
            test(41).should.be.true;
            test(40).should.be.true;
            test(39).should.be.true;
            test(30).should.be.true;
            test(29).should.be.false;
        });
    });

    describe('$lt', function () {
        it('should test numbers', function () {
            const test = p({ $lt: 40 });
            test(39).should.be.true;
            test(40).should.be.false;
            test(41).should.be.false;
        });

        it('should test dates', function () {
            const d1 = new Date('2016-01-01');
            const d2 = new Date('2017-01-01');
            const d3 = new Date('2018-01-01');

            const test = p({ $lt: d2 });
            test(d1).should.be.true;
            test(d2).should.be.false;
            test(d3).should.be.false;
        });

        it(`should handle an array of 1 value as if it were singular`, () => {
            const test = p({ $lt: [40] });
            test(41).should.be.false;
            test(40).should.be.false;
            test(39).should.be.true;
            test(30).should.be.true;
        });

        it(`should handle an array of values for the predicate as a disjunction [x lt N OR x lt M ...]`, () => {
            const test = p({ $lt: [40, 30] });
            test(41).should.be.false;
            test(40).should.be.false;
            test(39).should.be.true;
            test(30).should.be.true;
            test(29).should.be.true;
        });
    });

    describe('$lte', function () {
        it('should test numbers', function () {
            const test = p({ $lte: 40 });
            test(39).should.be.true;
            test(40).should.be.true;
            test(41).should.be.false;
        });

        it('should test dates', function () {
            const d1 = new Date('2016-01-01');
            const d2 = new Date('2017-01-01');
            const d3 = new Date('2018-01-01');

            const test = p({ $lte: d2 });
            test(d1).should.be.true;
            test(d2).should.be.true;
            test(d3).should.be.false;
        });

        it(`should handle an array of 1 value as if it were singular`, () => {
            const test = p({ $lte: [40] });
            test(41).should.be.false;
            test(40).should.be.true;
            test(39).should.be.true;
            test(30).should.be.true;
        });

        it(`should handle an array of values for the predicate as a disjunction [x lte N OR x lte M ...]`, () => {
            const test = p({ $lte: [40, 30] });
            test(41).should.be.false;
            test(40).should.be.true;
            test(39).should.be.true;
            test(30).should.be.true;
            test(29).should.be.true;
        });
    });

    describe('$notIn', function () {
        it('should test a set', function () {
            const test = p({ $notIn: [1, 2, 3] });
            test(1).should.be.false;
            test(4).should.be.true;
        });
    });

    describe(`$between`, () => {
        it(`should test numbers`, () => {
            const test = p({ $between: [0, 5] });
            test(-1).should.be.false;
            test(0).should.be.true;
            test(3).should.be.true;
            test(5).should.be.true;
            test(6).should.be.false;
        });

        it(`should test dates`, () => {
            const d0 = new Date('2015-01-01');
            const d1 = new Date('2016-01-01');
            const d2 = new Date('2017-01-01');
            const d3 = new Date('2018-01-01');
            const d4 = new Date('2019-01-01');

            const test = p({ $between: [d1, d3] });
            test(d0).should.be.false;
            test(d1).should.be.true;
            test(d2).should.be.true;
            test(d3).should.be.true;
            test(d4).should.be.false;
        });

        it(`should not care about hi/lo ordering`, () => {
            const test = p({ $between: [5, 0] });
            test(-1).should.be.false;
            test(0).should.be.true;
            test(3).should.be.true;
            test(5).should.be.true;
            test(6).should.be.false;
        });
    });

    describe(`$notBetween`, () => {
        it(`should test numbers`, () => {
            const test = p({ $notBetween: [0, 5] });
            test(-1).should.be.true;
            test(0).should.be.false;
            test(3).should.be.false;
            test(5).should.be.false;
            test(6).should.be.true;
        });

        it(`should test dates`, () => {
            const d0 = new Date('2015-01-01');
            const d1 = new Date('2016-01-01');
            const d2 = new Date('2017-01-01');
            const d3 = new Date('2018-01-01');
            const d4 = new Date('2019-01-01');

            const test = p({ $notBetween: [d1, d3] });
            test(d0).should.be.true;
            test(d1).should.be.false;
            test(d2).should.be.false;
            test(d3).should.be.false;
            test(d4).should.be.true;
        });
    });

    describe(`$like`, () => {
        it(`should match against a string`, () => {
            const test = p({ $like: `foo` });

            test('foo').should.be.true;
            test('fo').should.be.false;
            test('ooffoo').should.be.true;
            test('bar').should.be.false;
        });

        it(`should match against a string meant to be a regexp`, () => {
            const test = p({ $like: `foo\\.bar` });
            test('foo.bar').should.be.true
        });

        it(`should match against a literal regex`, () => {
            const test = p({ $like: /foo/ });
            test('foo').should.be.true;
            test('fo').should.be.false;
            test('fofo').should.be.false;
            test('osfasfasfoofdsfsaf').should.be.true;
            test('bar').should.be.false;
        });

        it(`should match against an instantiated RegExp object`, () => {
            const test = p({ $like: new RegExp('foo') });
            test('foo').should.be.true;
            test('fo').should.be.false;
            test('fofo').should.be.false;
            test('osfasfasfoofdsfsaf').should.be.true;
            test('bar').should.be.false;
        });

        it(`should force case-sensitive matching`, () => {
            const test = p({ $like: new RegExp('FoO', 'i') });
            const test1 = p({ $like: /FoO/i });

            test('foo').should.be.false;
            test('FOO').should.be.false;
            test('Foo').should.be.false;
            test('FoO').should.be.true;
            test('bar').should.be.false;

            test1('foo').should.be.false;
            test1('FOO').should.be.false;
            test1('Foo').should.be.false;
            test1('FoO').should.be.true;
            test1('bar').should.be.false;
        });
    });

    describe(`$notLike`, () => {
        it(`should match against a string`, () => {
            const test = p({ $notLike: `foo` });

            test('foo').should.be.false;
            test('fo').should.be.true;
            test('ooffoo').should.be.false;
            test('bar').should.be.true;
        });

        it(`should match against a literal regex`, () => {
            const test = p({ $notLike: /foo/ });
            test('foo').should.be.false;
            test('fo').should.be.true;
            test('fofo').should.be.true;
            test('osfasfasfoofdsfsaf').should.be.false;
            test('bar').should.be.true;
        });

        it(`should match against an instantiated RegExp object`, () => {
            const test = p({ $notLike: new RegExp('foo') });
            test('foo').should.be.false;
            test('fo').should.be.true;
            test('fofo').should.be.true;
            test('osfasfasfoofdsfsaf').should.be.false;
            test('bar').should.be.true;
        });

        it(`should force case-sensitive matching`, () => {
            const test = p({ $notLike: new RegExp('FoO', 'i') });
            const test1 = p({ $notLike: /FoO/i });

            test('foo').should.be.true;
            test('FOO').should.be.true;
            test('Foo').should.be.true;
            test('FoO').should.be.false;
            test('bar').should.be.true;

            test1('foo').should.be.true;
            test1('FOO').should.be.true;
            test1('Foo').should.be.true;
            test1('FoO').should.be.false;
            test1('bar').should.be.true;
        });
    });

    describe(`$ilike`, () => {
        it(`should match against a string`, () => {
            const test = p({ $ilike: `foo` });

            test('foo').should.be.true;
            test('fo').should.be.false;
            test('ooffoo').should.be.true;
            test('FOO').should.be.true;
            test('bar').should.be.false;
        });

        it(`should match against a literal regex`, () => {
            const test = p({ $ilike: /foo/i });
            test('foo').should.be.true;
            test('fOo').should.be.true;
            test('fo').should.be.false;
            test('fofo').should.be.false;
            test('osfasfasFoofdsfsaf').should.be.true;
            test('bar').should.be.false;
        });

        it(`should match against an instantiated RegExp object`, () => {
            const test = p({ $ilike: new RegExp('FOO', 'i') });
            test('foo').should.be.true;
            test('FoO').should.be.true;
            test('fo').should.be.false;
            test('fofo').should.be.false;
            test('osfasfasfoofdsfsaf').should.be.true;
            test('bar').should.be.false;
        });

        it(`should force case-insensitive matching`, () => {
            const test = p({ $ilike: new RegExp('FoO') });
            const test1 = p({ $ilike: /FoO/ });

            test('foo').should.be.true;
            test('FOO').should.be.true;
            test('Foo').should.be.true;
            test('FoO').should.be.true;
            test('bar').should.be.false;

            test1('foo').should.be.true;
            test1('FOO').should.be.true;
            test1('Foo').should.be.true;
            test1('FoO').should.be.true;
            test1('bar').should.be.false;
        });
    });

    describe(`$notIlike`, () => {
        it(`should match against a string`, () => {
            const test = p({ $notIlike: `foo` });

            test('foo').should.be.false;
            test('fo').should.be.true;
            test('ooffoo').should.be.false;
            test('FOO').should.be.false;
            test('bar').should.be.true;
        });

        it(`should match against a literal regex`, () => {
            const test = p({ $notIlike: /foo/i });
            test('foo').should.be.false;
            test('fOo').should.be.false;
            test('fo').should.be.true;
            test('fofo').should.be.true;
            test('osfasfasFoofdsfsaf').should.be.false;
            test('bar').should.be.true;
        });

        it(`should match against an instantiated RegExp object`, () => {
            const test = p({ $notIlike: new RegExp('FOO', 'i') });
            test('foo').should.be.false;
            test('FoO').should.be.false;
            test('fo').should.be.true;
            test('fofo').should.be.true;
            test('osfasfasfoofdsfsaf').should.be.false;
            test('bar').should.be.true;
        });

        it(`should force case-insensitive matching`, () => {
            const test = p({ $notIlike: new RegExp('FoO') });
            const test1 = p({ $notIlike: /FoO/ });

            test('foo').should.be.false;
            test('FOO').should.be.false;
            test('Foo').should.be.false;
            test('FoO').should.be.false;

            test1('foo').should.be.false;
            test1('FOO').should.be.false;
            test1('Foo').should.be.false;
            test1('FoO').should.be.false;
            test('bar').should.be.true;
        });
    });

    describe(`$contains`, () => {
        it(`should test a single value`, () => {
            const test = p({ $contains: 2 });
            test([2, 3, 4, 6]).should.be.true;
            test({ foo: 2 }).should.be.true;
            test({ 2: 'bar' });
            test([null, null, 'a']).should.be.false;
            test('2').should.be.true;
            test(['2']).should.be.false;
            test(2).should.be.true;
            test(22).should.be.false;
        });

        it(`should return true only if all values match if given many values to test`, () => {
            const test = p({ $contains: [3, 1] });
            test([1, 2, 3]).should.be.true;
            test({ foo: 1, bar: 2, baz: 3 }).should.be.true;
            test('13').should.be.true;
            test([3, 2, '1']).should.be.false;
            test(1).should.be.false;
        });

        it(`should allow mixed types when providing multiple values`, () => {
            const test = p({ $contains: [1, 'foo'] });
            test([2, 'foo', 4]).should.be.false;
            test('foo1').should.be.true;
            test(['foo', '1']).should.be.false;
            test(['foo', 'bar', 'baz', 3, 1, 2]).should.be.true;
            test({ foo: 'foo', 1: 'foo' }).should.be.false;
            test({ a: 'foo', b: 1 }).should.be.true;
        });

        it(`should test single string`, () => {
            const test = p({ $contains: 'foo' });
            test('foo').should.be.true;
            test(['foo']).should.be.true;
            test({ prop: 'foo' }).should.be.true;
            test({ foo: 'bar' }).should.be.false;
            test('123foo123').should.be.true;
        });
    });

    it(`should create a transform stream for filtering out streamed items using a predicate`, done => {
        const filter = { $or: [{ first: 'Brad' }, { last: 'Leupen' }] };

        const data = [
            { first: 'Brad', last: 'Leupen' },
            { first: 'Hank', last: 'Leupen' },
            { first: 'Hank', last: 'Azaria' },
            { first: 'Brad', last: 'Pitt' },
            { first: 'Bradd', last: 'LLeupen' }
        ];

        const vals = [];
        dataStream(data)
            .pipe(p.stream(filter))
            .pipe(new stream.Writable({
                objectMode: true,
                write (rec, enc, next) {
                    vals.push(rec);
                    next();
                }
            }))
            .on('error', done)
            .on('finish', () => {
                vals.should.have.length(3);
                done();
            });
    });

    it(`should properly filter via transform stream`, done => {
        const filter = {
            $and: [
                {
                    oi_sess_cr_id: {
                        $ne: ``
                    }
                },
                {
                    $or: [
                        {
                            oi_sess_cr_id: {
                                $eq: [
                                    `JAVA`,
                                    `PHP`
                                ]
                            }
                        },
                        {
                            oi_sess_cr_id: {
                                $like: `.*ADOBE.*`
                            }
                        }
                    ]
                }
            ]
        };

        const data = [
            { name: 'Foo', oi_sess_cr_id: 'JAVA' },
            { name: 'Foo', oi_sess_cr_id: '' },
            { name: 'Foo', oi_sess_cr_id: 'ADOBEPHOTO' },
            { name: 'Foo', oi_sess_cr_id: 'PHP' },
            { name: 'Foo', oi_sess_cr_id: 'TERR' },
            { name: 'Foo', oi_sess_cr_id: 'ADOBE' },
            { name: 'Foo', oi_sess_cr_id: 'JAVASCRIPT' },
            { name: 'Foo', oi_sess_cr_id: 'INFORMER' },
        ];

        const vals = [];
        dataStream(data)
            .pipe(p.stream(filter))
            .pipe(new stream.Writable({
                objectMode: true,
                write (rec, enc, next) {
                    vals.push(rec);
                    next();
                }
            }))
            .on('error', done)
            .on('finish', () => {
                vals.should.have.length(4);
                done();
            });
    });

    it(`should behave like a passthrough when an undefined filter is passed to .stream()`, done => {
        const data = [
            { name: 'Foo', oi_sess_cr_id: 'JAVA' },
            { name: 'Foo', oi_sess_cr_id: '' },
            { name: 'Foo', oi_sess_cr_id: 'ADOBEPHOTO' },
            { name: 'Foo', oi_sess_cr_id: 'PHP' },
            { name: 'Foo', oi_sess_cr_id: 'TERR' },
            { name: 'Foo', oi_sess_cr_id: 'ADOBE' },
            { name: 'Foo', oi_sess_cr_id: 'JAVASCRIPT' },
            { name: 'Foo', oi_sess_cr_id: 'INFORMER' },
        ];

        const vals = [];
        dataStream(_.cloneDeep(data))
            .pipe(p.stream())
            .pipe(new stream.Writable({
                objectMode: true,
                write (rec, enc, next) {
                    vals.push(rec);
                    next();
                }
            }))
            .on('error', done)
            .on('finish', () => {
                vals.should.have.length(8);
                vals.should.deep.equal(data);
                done();
            });
    });

    it(`should allow leniency to pass property tests when a record does not have the property`, () => {


    });
});