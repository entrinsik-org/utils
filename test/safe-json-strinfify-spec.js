'use strict';

const utils = require('../lib');

describe('safe json stringify', function () {

    it('should safely stringify a circular object structure', function () {

        const andrea = { name: 'Andrea' };
        const carl = { name: 'Carl' };
        andrea.spouse = carl;
        carl.spouse = andrea;

        const andrea2 = JSON.parse(utils.safeJsonStringify(andrea));
        andrea2.should.deep.equal({ name: 'Andrea', spouse: { name: 'Carl' } });

    });

});