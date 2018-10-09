'use strict';

const _ = require('lodash');
const { create, terms, sum, avg, cardinality, docCount, dateHistogram, filter, bucketScript } = require('../lib/aggs.js');
const Client = require('elasticsearch').Client;
const client = new Client();
require('chai').should();

describe('agg builder', function () {
    describe('building', function () {
        it('should build a state terms agg', function () {
            create().aggs(terms('state').as('state')).toJson().should.deep.equal({
                aggs: {
                    state: {
                        terms: {
                            field: 'state'
                        }
                    }
                }
            });
        });

        it('should build a state terms / city terms agg', function () {
            create().aggs({
                states: terms('state').aggs({
                    cities: terms('city')
                })
            }).toJson().should.deep.equal({
                aggs: {
                    states: {
                        terms: {
                            field: 'state'
                        },
                        aggs: {
                            cities: {
                                terms: {
                                    field: 'city'
                                }
                            }
                        }
                    }
                }
            })
        });

        it('should build a state terms / city terms / sum amount agg', function () {
            create().aggs(
                terms('state').as('states').aggs(
                    terms('city').as('cities').aggs(
                        sum('amount').as('total')
                    )
                )
            ).toJson().should.deep.equal({
                aggs: {
                    states: {
                        terms: {
                            field: 'state'
                        },
                        aggs: {
                            cities: {
                                terms: {
                                    field: 'city'
                                },
                                aggs: {
                                    total: {
                                        sum: {
                                            field: 'amount'
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            })
        });

        it('should build a state terms / city terms / [ sum amount, avg amount ] agg', function () {
            create().aggs(
                terms('state').as('states').aggs(
                    terms('city').as('cities').aggs(
                        sum('amount').as('total'),
                        avg('amount').as('average')
                    )
                )
            ).toJson().should.deep.equal({
                aggs: {
                    states: {
                        terms: {
                            field: 'state'
                        },
                        aggs: {
                            cities: {
                                terms: {
                                    field: 'city'
                                },
                                aggs: {
                                    total: {
                                        sum: {
                                            field: 'amount'
                                        }
                                    },
                                    average: {
                                        avg: {
                                            field: 'amount'
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
        });

        it('should nest an array of aggs into a tree', function () {
            create().nest(
                terms('state').as('states'),
                terms('city').as('cities'),
                [
                    sum('amount').as('total'),
                    avg('amount').as('average')
                ]
            ).toJson().should.deep.equal({
                aggs: {
                    states: {
                        terms: {
                            field: 'state'
                        },
                        aggs: {
                            cities: {
                                terms: {
                                    field: 'city'
                                },
                                aggs: {
                                    total: {
                                        sum: {
                                            field: 'amount'
                                        }
                                    },
                                    average: {
                                        avg: {
                                            field: 'amount'
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            })
        });

        it('should expand an array of nested parents', function () {
            create().nest(
                [ terms('salesperson').as('salespeople'), terms('product').as('products') ],
                terms('country').as('countries'),
                sum('amount').as('total')
            ).toJson().should.deep.equal({
                aggs: {
                    products: {
                        aggs: {
                            countries: {
                                aggs: {
                                    total: {
                                        sum: {
                                            field: "amount"
                                        }
                                    }
                                },
                                terms: {
                                    field: "country"
                                }
                            }
                        },
                        terms: {
                            field: "product"
                        }
                    },
                    salespeople: {
                        aggs: {
                            countries: {
                                aggs: {
                                    total: {
                                        sum: {
                                            field: "amount"
                                        }
                                    }
                                },
                                terms: {
                                    field: "country"
                                }
                            }
                        },
                        terms: {
                            field: "salesperson"
                        }
                    }
                }
            });
        });

        it('should support an agg built on the fly', function () {
            create()
                .agg(
                    create({ body: { sum: { field: 'amount' } } }).as('total')
                )
                .toJson()
                .should.deep.equal({
                "aggs": {
                    "total": {
                        "sum": {
                            "field": "amount"
                        }
                    }
                }
            });
        });

        it('should support an agg literal', function () {
            create()
                .aggs({ total_sales: sum('amount'), average_sales: avg('amount') })
                .toJson()
                .should.deep.equal({
                "aggs": {
                    "average_sales": {
                        "avg": {
                            "field": "amount"
                        }
                    },
                    "total_sales": {
                        "sum": {
                            "field": "amount"
                        }
                    }
                }
            })
        });

        it('should support a fully formed configuration', function () {
            create()
                .agg(terms({ field: 'ShipCountry', size: 100 }).as('countries'))
                .toJson()
                .should.deep.equal({
                aggs: {
                    countries: {
                        terms: {
                            field: 'ShipCountry',
                            size: 100
                        }
                    }
                }
            })
        });
    });

    describe('transforming', function () {
        let OPTS = { index: 'test-northwind-orders' };

        before(async function () {
            try {
                await client.indices.delete({ index: 'test-northwind-orders' });
            } catch (err) {
                // could already exist
            }
        });

        before(async function () {
            try {
                await client.indices.create({ index: 'test-northwind-orders' });
            } catch (err) {
                // could already exist
            }
        });

        before(async function () {
            try {
                await client.indices.putMapping({
                    index: 'test-northwind-orders',
                    type: 'data',
                    body: require('./northwind-orders-mapping.json')
                });
            } catch (err) {
                console.log(err);
            }
        });

        before(async function () {
            const data = require('./northwind-orders');
            const commands = _(data)
                .map(r => _.assign(r, {
                    OrderDate: new Date(r.OrderDate),
                    ShippedDate: new Date(r.ShippedDate),
                    RequiredDate: new Date(r.RequiredDate)
                }))
                .map(r => [ { index: { _index: 'test-northwind-orders', _type: 'data' } }, r ])
                .flatten()
                .value();

            await client.bulk({ body: commands });
            await client.indices.flush({ index: 'test-northwind-orders' });
        });

        it('should support a single metric', async function () {
            const result = await create().agg(sum('orderAmount').as('total')).search(client, OPTS);
            result.should.have.property('total').that.is.closeTo(1343871.39, 0.001);
        });

        it('should have a default bucket mapper', async function () {
            const result = await create()
                .aggs({
                    countries: terms('ShipCountry').aggs({
                        distinctCities: cardinality('ShipCity')
                    })
                })
                .get('countries')
                .search(client, OPTS);

            result.should.deep.equal([
                {
                    'doc_count': 352,
                    'key': 'USA',
                    'distinctCities': 12
                },
                {
                    'doc_count': 326,
                    'key': 'Germany',
                    'distinctCities': 11
                },
                {
                    'doc_count': 197,
                    'key': 'Brazil',
                    'distinctCities': 4
                },
                {
                    'doc_count': 178,
                    'key': 'France',
                    'distinctCities': 9
                },
                {
                    'doc_count': 135,
                    'key': 'UK',
                    'distinctCities': 3
                },
                {
                    'doc_count': 125,
                    'key': 'Austria',
                    'distinctCities': 2
                },
                {
                    'doc_count': 118,
                    'key': 'Venezuela',
                    'distinctCities': 4
                },
                {
                    'doc_count': 97,
                    'key': 'Sweden',
                    'distinctCities': 2
                },
                {
                    'doc_count': 75,
                    'key': 'Canada',
                    'distinctCities': 3
                },
                {
                    'doc_count': 72,
                    'key': 'Mexico',
                    'distinctCities': 1
                }
            ]);
        });

        it('should support a bucket agg with a bucket mapper', async function () {
            const result = await create()
                .agg(
                    terms('ShipCountry').as('countries').map(b => [ b.key, b.distinct, b.doc_count ]).agg(
                        cardinality('ShipCity').as('distinct')
                    )
                )
                .search(client, OPTS);

            result.countries.should.deep.equal([
                [ 'USA', 12, 352 ],
                [ 'Germany', 11, 326 ],
                [ 'Brazil', 4, 197 ],
                [ 'France', 9, 178 ],
                [ 'UK', 3, 135 ],
                [ 'Austria', 2, 125 ],
                [ 'Venezuela', 4, 118 ],
                [ 'Sweden', 2, 97 ],
                [ 'Canada', 3, 75 ],
                [ 'Mexico', 1, 72 ]
            ]);
        });

        it('should support a bucket agg with bucket reducer', async function () {
            const result = await create()
                .agg(terms('ShipCountry').as('countries').reduce((acc, b) => _.set(acc, [ b.key ], b.doc_count), {}))
                .search(client, OPTS);

            result.should.deep.equal({
                countries: {
                    Austria: 125,
                    Brazil: 197,
                    Canada: 75,
                    France: 178,
                    Germany: 326,
                    Mexico: 72,
                    Sweden: 97,
                    UK: 135,
                    USA: 352,
                    Venezuela: 118
                }
            });
        });

        it('should support a bucket agg with bucket reducer and child aggs', async function () {
            const result = await create()
                .aggs({
                    countries: terms('ShipCountry').aggs({
                        count: docCount(),
                        total: sum('orderAmount').round()
                    }).reduce((acc, b) => _.set(acc, [ b.key ], [ b.count, b.total ]), {})
                })
                .search(client, OPTS);

            result.should.deep.equal({
                countries: {
                    Austria: [
                        125,
                        139497
                    ],
                    Brazil: [
                        197,
                        111711
                    ],
                    Canada: [
                        75,
                        55334
                    ],
                    France: [
                        178,
                        84388
                    ],
                    Germany: [
                        326,
                        242777
                    ],
                    Mexico: [
                        72,
                        24073
                    ],
                    Sweden: [
                        97,
                        59524
                    ],
                    UK: [
                        135,
                        60617
                    ],
                    USA: [
                        352,
                        263567
                    ],
                    Venezuela: [
                        118,
                        60815
                    ]
                }
            });
        });

        it('should support a key/value agg structure', async function () {
            const agg = create()
                .aggs({
                    sales_per_month: dateHistogram({ field: 'OrderDate', interval: 'month' }).aggs({
                        total_sales: sum('orderAmount').round(2),
                        swiss_sales: filter({ term: { ShipCountry: 'Switzerland' } }).aggs({
                            sales: sum('orderAmount').round(2)
                        }).get('sales'),
                        swiss_pct: bucketScript({
                            buckets_path: {
                                swissSales: 'swiss_sales>sales',
                                totalSales: 'total_sales'
                            },
                            script: 'params.swissSales / params.totalSales * 100'
                        }).round(2)
                    }).map(m => _.pick(m, ['key_as_string', 'swiss_sales', 'total_sales', 'swiss_pct']))
                })
                .get('sales_per_month')
                .first();
            const result = await agg.search(client, OPTS);
            result.should.deep.equal({
                "key_as_string": "1996-07-01T00:00:00.000Z",
                "swiss_sales": 2490.5,
                "total_sales": 19604.9,
                "swiss_pct": 12.7
            });
        });

        it('should support mapper chaining', async function () {
            const agg = create()
                .aggs({
                    sales: sum('orderAmount').thru(() => 100).thru(v => '$' + v)
                })
                .get('sales');
            const result = await agg.search(client, OPTS);
            result.should.equal('$100');
        });
    });
});