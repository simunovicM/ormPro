import { mapIdsFnc, parseValues } from './sqlCommon';
import { isArray } from 'util';
import { mapMany, all, any, groupBy } from './common';
let createDeleteStr = obj => {
    let objs = isArray(obj) ? obj : [obj];
    let mappFnc = mapIdsFnc(objs[0].__test.defObj, null);
    var mappedIds = objs.map(mappFnc);
    return 'Delete from ' + objs[0].__test.defObj.tableName + ' where '
        + mappedIds.map(mapp => '(' + mapp.map(f => f.name + '=' + parseValues(f)).join(' and ') + ')').join(' or ');
}

const deleteFnc = sqlDeleteFnc => {
    let createDeleteStrs = obj => {
        let objs = isArray(obj) ? obj : [obj];
        if (!any(objs)) return [];
        if (!all(objs, obj => obj != null && obj.__test)) throw Error('Cannot delete object that\'s not from the database!');
        let childs = mapMany(objs, obj => obj.__test.defObj.defsObjs.map(f => { if (obj[f.propName]) return { ...f, obj: obj[f.propName] }; }))
            .filter(f => f != null && f.shouldDelete && f.obj && f.obj.__test);
        let befores = groupBy(childs.filter(f => !f.insertBefore), 'objName');
        let afters = groupBy(childs.filter(f => f.insertBefore), 'objName');
        let ret = [
            ...mapMany(befores, group => createDeleteStrs(group.items.map(f => f.obj))),
            createDeleteStr(objs),
            ...mapMany(afters, group => createDeleteStrs(group.items.map(f => f.obj)))
        ];
        objs.forEach(f => delete (f.__test));
        return ret;
    };
    return obj => sqlDeleteFnc()(createDeleteStrs(obj).join(';\r\n'));
}
export default deleteFnc;