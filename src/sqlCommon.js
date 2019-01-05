import { isFunction, isObject, isArray } from 'util';

export const parseValues = val => {
    if (val.raw) {
        return val.raw;
    } else if (typeof val.value === 'boolean') {
        return val.value ? 1 : 0;
    } else if (typeof val.value === 'string')
        return 'N\'' + val.value + '\'';
    else if (val.value instanceof Date)
        return '\'' + val.value.toISOString() + '\'';
    else if (val.value === null)
        return 'null';
    else return val.value;
};
export const mappingFnc = (defObj, parent) => obj => defObj.columns.map(f => {
    return f.insertFnc ? { name: f.name, ...f.insertFnc(obj, parent) } : { name: f.name, value: obj[f.name] };
});
export const mapIdsFnc = (defObj, parent) => obj => defObj.columns.filter(f => defObj.ids.find(g => g.name === f.name)).map(f => {
    return f.insertFnc ? { name: f.name, ...f.insertFnc(obj, parent) } : { name: f.name, value: obj[f.name] };
});
export const checkEquals = (defaultObj, obj) => {
    for (var key in defaultObj) {
        if (defaultObj[key] !== undefined && obj[key] === undefined) return false;
        if (isObject(defaultObj[key])) {
            if (!checkEquals(defaultObj[key], obj[key]))
                return false;
        }
        else if (!isFunction(defaultObj[key]) && defaultObj[key] !== obj[key]) return false;
    }
    return true;
}
export const attachIsModified = (defaultObj, defObj, parent) => {
    let mapping = mappingFnc(defObj, parent);
    defaultObj.__test = defaultObj.__test || {};

    let defaultMapping = mapping(defaultObj);

    defaultObj.__test.isModified = () => {
        let objMapping = mapping(defaultObj);
        for (var i = 0; i < defaultMapping.length; i++)
            if (defaultMapping[i].name !== objMapping[i].name || defaultMapping[i].value !== objMapping[i].value)
                return true;
        return false;
    }
    defaultObj.__test.mapping = mapping;
    defaultObj.__test.defObj = { ...defObj };
    return defaultObj;
};

export const splitMultiData = data => {
    let datas = [];
    let last = data.reduce((acc, f) => {
        if (f.test === 1) {
            for (var key in f)
                if (key !== 'test') {
                    acc.push(f);
                    return acc;
                }
            datas.push(acc);
            return [];
        } else {
            acc.push(f);
            return acc;
        }
    }, []);
    if (last.length > 0) datas.push(last);
    return datas;
}

export const mergeData = (from, to) => {
    for (var key in from) {
        if (to[key] === undefined)
            to[key] = from[key];
        else {
            if (isObject(from[key]))
                mergeData(from[key], to[key]);
            else if (isArray(from[key])) {
                throw new Error('Cannot merge this!');
            }
            else if (!isFunction(from[key]) && to[key] == null)
                to[key] = from[key];
        }
    }
    return to;
}

let isEmptyObj = obj => {
    if (obj.__test === undefined) {
        for (var key in obj) {
            if (isObject(obj[key])) {
                if (!isEmptyObj(obj[key])) return false;
            } else if (obj[key] != null) return false;
        }
        return true;
    }
    return false;
}
export const removeEmptyRows = obj => {
    for (var key in obj) {
        if (isArray(obj[key])) {
            obj[key] = obj[key].filter(f => !isEmptyObj(f));
            if (obj[key].length === 0)
                delete obj[key];
        } else if (isObject(obj[key])) {
            if (isEmptyObj(obj[key]))
                delete obj[key];
            else removeEmptyRows(obj[key]);
        }
    }
    return obj;
}