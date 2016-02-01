'use strict';

var URIjs = require('urijs');
var _ = require('lodash');

function Collection(items, start, total) {
    this.items = items || [];
    this.start = start || 0;
    this.count = this.items.length;
    this.total = total >= 0 ? total : this.items.length;
}

Collection.prototype.toHal = function(rep, done) {
    var limit = Number(rep.request.query.limit) || 10;
    var uri = new URIjs(rep.self);
    var prev = Math.max(0, this.start - limit);
    var next = Math.min(this.total, this.start + limit);

    var query = uri.search(true);

    if (this.start > 0) {
        rep.link('prev', uri.search(_.assign(query, { start: prev, limit: limit })).toString());
    }
    if (this.start + this.count < this.total) {
        rep.link('next', uri.search(_.assign(query, { start: next, limit: limit })).toString());
    }
    done();
};

exports.Collection = Collection;