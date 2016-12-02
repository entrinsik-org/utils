'use strict';

var joi = require('joi');

var internals = {};

internals.opSchema = {
    path: joi.string().required(),
    op: joi.string().required().valid(['add', 'remove', 'replace', 'copy', 'move', 'test']),
    // value: joi.any().when('op', { is: ['add', 'replace', 'test'], otherwise: joi.forbidden()}),
    // from: joi.string().when('op', { is: ['copy', 'move'], otherwise: joi.forbidden()})
    value: joi.any(),
    from: joi.string()
};

internals.schema = joi.array().items(internals.opSchema).default([]);

module.exports = internals.schema;