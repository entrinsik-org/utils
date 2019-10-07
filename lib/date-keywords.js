'use strict';

const moment = require('moment-timezone');
const _ = require('lodash');

function DateKeywords (dateKeyword) {

    const tz = {
        transform: function (input, timezone) {
            const date = _.isString(input) ? moment(input).toDate() : input;
            const str = moment(date).format('YYYY-MM-DD');
            // must be in format tz(str, timezone) and NOT moment(str).tz(timezone). the latter adjusts the date
            return moment.tz(str, timezone);
        },
        effective: dateKeyword.tz
    };

    const parser = function (param) {
        const operator = param.replace(/[^+\-]+/g, '');
        const number = operator && operator !== '+' ? '-' + param.replace(/[^0-9]+/g, '') : param.replace(/[^0-9]+/g, '');
        const unit = operator ? param.split(operator)[1].replace(/[^a-zA-Z]/g, '') : '';
        return {
            unit: unit,
            number: Number(number)
        };
    };
    const makeDate = function (date) {
        return tz.transform(date, tz.effective);
    };
    const keywords = {
        NOW: function (param) {
            if (!param) return moment();
            const info = parser(param);
            return moment().add(info.number, info.unit || 'd');
        },
        TODAY: function (param) {
            if (!param) return makeDate(moment());
            const info = parser(param);
            return makeDate(moment()).add(info.number, info.unit || 'd');
        },
        YESTERDAY: function (param) {
            if (!param) return makeDate(moment()).add(-1, 'd');
            const info = parser(param);
            return makeDate(moment()).add(-1, 'd').add(info.number, info.unit || 'd');
        },
        WEEK_BEGIN: function (param) {
            if (!param) return makeDate(moment()).startOf('week');
            const info = parser(param);
            return makeDate(moment()).startOf('week').add(info.number, info.unit || 'd');
        },
        WEEK_END: function (param) {
            if (!param) return makeDate(moment()).endOf('week');
            const info = parser(param);
            return makeDate(moment()).endOf('week').add(info.number, info.unit || 'd');
        },
        MONTH_BEGIN: function (param) {
            const date = new Date();
            if (!param) return tz.transform(new Date(date.getFullYear(), date.getMonth(), 1), tz.effective);
            const info = parser(param);
            return tz.transform(new Date(date.getFullYear(), date.getMonth(), 1), tz.effective).add(info.number, info.unit || 'M');
        },
        MONTH_END: function (param) {
            const date = new Date();
            if (!param) return tz.transform(new Date(date.getFullYear(), date.getMonth() + 1, 0), tz.effective);
            const info = parser(param);
            if (!info.unit || info.unit.toUpperCase() === 'MONTH' || info.unit === 'M')
                return tz.transform(new Date(date.getFullYear(), date.getMonth(), 1), tz.effective).add(Number(info.number) + 1, 'M').add(-1, 'd');
            return tz.transform(new Date(date.getFullYear(), date.getMonth() + 1, 0), tz.effective).add(info.number, info.unit || 'M');
        },
        YEAR_BEGIN: function (param) {
            const date = new Date();
            if (!param) return tz.transform(new Date(date.getFullYear()), tz.effective);
            const info = parser(param);
            return tz.transform(new Date(date.getFullYear(), 0, 1), tz.effective).add(info.number, info.unit || 'year');
        },
        YEAR_END: function (param) {
            const date = new Date();
            if (!param) return tz.transform(new Date(date.getFullYear(), 11, 31), tz.effective);
            const info = parser(param);
            return tz.transform(new Date(date.getFullYear(), 11, 31), tz.effective).add(info.number, info.unit || 'year');
        },
        QTR_BEGIN: function (param) {
            const date = new Date();
            let date2 = moment();
            switch (date.getMonth()) {
                case 0:
                case 1:
                case 2:
                    date2 = tz.transform(date.getFullYear() + '-01-01', tz.effective);
                    break;
                case 3:
                case 4:
                case 5:
                    date2 = tz.transform(date.getFullYear() + '-04-01', tz.effective);
                    break;
                case 6:
                case 7:
                case 8:
                    date2 = tz.transform(date.getFullYear() + '-07-01', tz.effective);
                    break;
                default:
                    date2 = tz.transform(date.getFullYear() + '-10-01', tz.effective);
                    break;
            }
            if (!param) return date2;
            const info = parser(param);
            return date2.add(info.number, info.unit || 'Q');
        },
        QTR_END: function (param) {
            const date = new Date();
            let date2 = moment();
            switch (date.getMonth()) {
                case 0:
                case 1:
                case 2:
                    date2 = tz.transform(date.getFullYear() + '-03-31', tz.effective);
                    break;
                case 3:
                case 4:
                case 5:
                    date2 = tz.transform(date.getFullYear() + '-06-30', tz.effective);
                    break;
                case 6:
                case 7:
                case 8:
                    date2 = tz.transform(date.getFullYear() + '-09-30', tz.effective);
                    break;
                default:
                    date2 = tz.transform(date.getFullYear() + '-12-31', tz.effective);
                    break;
            }
            if (!param) return date2;
            const info = parser(param);
            return date2.add(info.number, info.unit || 'Q');
        },
        MONTH_AGO: function (param) {
            const date = new Date();
            if (!param) return tz.transform(new Date(date.getFullYear(), date.getMonth() - 1, date.getDate()), tz.effective);
            const info = parser(param);
            return tz.transform(moment({
                year: date.getFullYear(),
                month: date.getMonth(),
                day: date.getDate()
            }).add(-1, 'month'), tz.effective).add(info.number, info.unit || 'M');
        },
        MONTH_FROM_NOW: function (param) {
            const date = new Date();
            if (!param) return tz.transform(new Date(date.getFullYear(), date.getMonth() + 1, date.getDate()), tz.effective);
            const info = parser(param);
            return tz.transform(moment({
                year: date.getFullYear(),
                month: date.getMonth(),
                day: date.getDate()
            }).add(1, 'month'), tz.effective).add(info.number, info.unit || 'M');
        },
        YEAR_AGO: function (param) {
            const date = new Date();
            if (!param) return tz.transform(new Date(date.getFullYear() - 1, date.getMonth(), date.getDate()), tz.effective);
            const info = parser(param);
            return tz.transform(moment({
                year: date.getFullYear(),
                month: date.getMonth(),
                day: date.getDate()
            }).add(-1, 'year'), tz.effective).add(info.number, info.unit || 'year');
        },
        YEAR_FROM_NOW: function (param) {
            const date = new Date();
            if (!param) return tz.transform(new Date(date.getFullYear() + 1, date.getMonth(), date.getDate()), tz.effective);
            const info = parser(param);
            return tz.transform(moment({
                year: date.getFullYear(),
                month: date.getMonth(),
                day: date.getDate()
            }).add(1, 'year'), tz.effective).add(info.number, info.unit || 'year');
        }
    };

    const parse = function (v) {
        v = v.replace(/\s/g, '');
        if (v.split('+').length > 1)
            return keywords[v.split('+')[0].toUpperCase()](v);
        else if (v.split('-').length > 1)
            return keywords[v.split('-')[0].toUpperCase()](v);
        else
            return keywords[v.toUpperCase()]();
    };

    return parse(dateKeyword.value);

}

exports.DateKeywords = DateKeywords;