'use strict';

const chai = require('chai');
const should = chai.should();
const dateKeywords = require('../lib/date-keywords').DateKeywords;
const moment = require('moment-timezone');

describe('date keywords', function() {
    describe('YEAR_BEGIN', function() {
        it('should default to the beginning of the current year without a modifier', function() {
            let result = dateKeywords({
                "value": "YEAR_BEGIN",
                "operator": "gte",
                "tz": "America/New_York"
            });

            moment(result).format('YYYY-MM-DD').should.equal(`${new Date().getFullYear()}-01-01`);
        });
    });
});