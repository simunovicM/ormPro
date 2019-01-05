let isFunction = function (obj) { return (typeof obj === "function"); }
export const isObject = function (obj) { return (typeof obj === "object"); }
export const replaceAll = function (str, fromString, toString) {
    return str.replace(new RegExp(escapeRegExp(fromString), 'g'), toString);
};
function escapeRegExp(str) {
    return str.replace(/([.*+?^=!:${}()|[\]\\])/g, "\\$1");
}
export const tail = function (arr) {
    return arr.filter(function (_, ind) { return ind > 0 });
};

export const mapMany = (arr, mapper) => arr.reduce((prev, curr) => prev.concat(mapper(curr)), []);
export const any = function (arr, fnc) {
    if (fnc == null) return arr.length > 0;
    return arr.find(fnc) !== undefined;
};
export const all = function (arr, fnc) {
    return arr.filter(fnc).length === arr.length;
};
export function hashCode(str) {
    // var str = JSON.stringify(obj);
    var hash = 0;
    if (str.length === 0) return hash;
    for (let i = 0; i < str.length; i++) {
        hash = ((hash << 5) - hash) + str.charCodeAt(i);
        hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
}
export const objectToKeyValueList = obj => {
    var retObj = [];
    for (let key in obj)
        retObj.push({ key: key, value: obj[key] });
    return retObj;
};

export const matchAll = function (str, findStr) {
    var ret = [];
    var re = new RegExp(findStr, 'g');
    var m;
    do {
        m = re.exec(str);
        if (m) {
            ret.push({ value: m[0], indices: [m.index, m.index + m[0].length] });
        }
    } while (m);
    return ret;
}

export const splitTextByIndices = (text, extracts, lastTextPosition) => {
    if (extracts.length === 0) {
        if (!lastTextPosition) return [text];
        return [text.substring(lastTextPosition, text.length)];
    }
    return [
        text.substring(lastTextPosition || 0, extracts[0].indices[0]),
        extracts[0].text,
        ...splitTextByIndices(text, tail(extracts), extracts[0].indices[1])
    ];
}

export const groupBy = function (arr, prop) {
    var propFnc = (!isFunction(prop)) ? function (f) { return f[prop]; } : prop;
    return arr.reduce(function (groups, item) {
        var val = propFnc(item);
        var find = groups.find(function (f) { return f.key == val; });
        if (find != null)
            find.items.push(item);
        else
            groups.push({ key: val, items: [item] });
        return groups;
    }, []);
};