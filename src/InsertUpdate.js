import { objectToKeyValueList, mapMany } from './common';
import { parseValues, attachIsModified, mappingFnc } from './sqlCommon';

const insertUpdateFnc = (definitions, sqlInsertFnc) => {
    let insertEachObject = (inserts, objContainer) => {
        let allObjs = mapMany(inserts, insert => (insert.isArray ? objContainer[insert.propName] : [objContainer[insert.propName]]).map(f => ({ def: insert, obj: f })));
        let count = 0;
        let doNext = () => {
            let { def, obj } = allObjs[count];
            count++;
            let goToNext = () => {
                if (count > allObjs.length - 1)
                    thenFnc(objContainer);
                else doNext();
            }
            if (obj.__test) {
                if (!obj.__test.isModified(obj))
                    goToNext();
                else {
                    updateFnc(def.objName, obj, objContainer)
                        .then(_ => {
                            if (def.onInsertFnc) def.onInsertFnc(objContainer);
                            goToNext();
                        })
                }
            }
            else
                insertFnc(def.objName, obj, objContainer)
                    .then(_ => {
                        if (def.onInsertFnc) def.onInsertFnc(objContainer);
                        goToNext();
                    });
        }
        let thenFnc = _ => null;
        return { then: fnc => { thenFnc = fnc; doNext(); } };
    }
    let shouldBeInserted = (objName, obj) => {
        let defObj = definitions.find(f => f.objName === objName);
        return objectToKeyValueList(obj).find(f => defObj.ids.find(g => g.name === f.key) === undefined) != null;
    }

    let insertUpdateStr = (defObj, obj, parent) => {
        let mapping = mappingFnc(defObj, parent)(obj);
        let filterMapping = mapping => mapping.filter(f => (f.value !== undefined || f.raw !== undefined) && defObj.ids.find(g => !g.insert && f.name === g.name) == null);
        let insertStr = () => {
            return 'insert into ' + defObj.tableName + ' (' + filterMapping(mapping).map(f => f.name).join(',') + ')'
                + '\r\nvalues (' + filterMapping(mapping).map(f => parseValues(f)).join(',') + ');'
                + (defObj.ids.filter(f => !f.insert).length > 0 ? '\r\nSELECT @@IDENTITY AS \'Identity\';' : '');
        }
        let updateStr = () => {
            return 'update ' + defObj.tableName + ' set '
                + filterMapping(mapping).map(f => f.name + ' = ' + parseValues(f)).join(', ')
                + '\r\nwhere ' + defObj.ids.map(f => f.name + ' = ' + parseValues(mapping.find(g => g.name === f.name))).join(' and ');
        }
        return { insertStr: insertStr, updateStr: updateStr };
    };

    let insertFnc = (name, obj, parent) => {
        let defObj = definitions.find(f => f.objName === name);
        if (defObj == null) throw new Error('could not find definition for: ' + name);
        let insertMe = () => {
            sqlInsertFnc()(insertUpdateStr(defObj, obj, parent).insertStr()).then(response => {
                if (response.data.name === 'SequelizeDatabaseError')
                    throw new Error('could not insert: ' + name + '\r\n' + response.data.original.message);

                defObj.ids.filter(id => !id.insert).forEach((id, ind) => {
                    let col = defObj.columns.find(f => f.name === id.name);
                    if (col.parseFnc) {
                        debugger;
                    } else obj[col.name] = response.data[ind].Identity;
                })
                obj = attachIsModified(obj, defObj, parent);

                let insertAfter = defObj.defsObjs.filter(f => !f.insertBefore && obj[f.propName] && shouldBeInserted(f.objName, obj[f.propName]));

                if (insertAfter.length > 0) {
                    insertEachObject(insertAfter, obj)
                        .then(() => thenFnc(obj));
                } else thenFnc(obj);
            });
            let thenFnc = _ => null;
            return { then: fnc => thenFnc = fnc };
        };

        let insertBefore = defObj.defsObjs.filter(f => f.insertBefore && obj[f.propName] && shouldBeInserted(f.objName, obj[f.propName]));

        if (insertBefore.length > 0) {
            insertEachObject(insertBefore.map(f => ({ ...f })), obj)
                .then(() => insertMe().then(_ => thenFnc(obj)));
            let thenFnc = _ => null;
            return { then: fnc => thenFnc = fnc };
        } else return insertMe();
    }
    let updateFnc = (name, obj, parent) => {
        let defObj = definitions.find(f => f.objName === name);
        if (defObj == null) throw new Error('could not find definition for: ' + name);
        let updateMe = () => {
            sqlInsertFnc()(insertUpdateStr(defObj, obj, parent).updateStr()).then(response => {
                if (response.data.name === 'SequelizeDatabaseError')
                    throw new Error('could not update: ' + name + '\r\n' + response.data.original.message);
                obj = attachIsModified(obj, defObj, parent);

                let insertAfter = defObj.defsObjs.filter(f => !f.insertBefore && obj[f.propName] && shouldBeInserted(f.objName, obj[f.propName]));

                if (insertAfter.length > 0) {
                    insertEachObject(insertAfter, obj)
                        .then(() => thenFnc(obj));
                } else thenFnc(obj);
            });
            let thenFnc = _ => null;
            return { then: fnc => thenFnc = fnc };
        };

        let insertBefore = defObj.defsObjs.filter(f => f.insertBefore && obj[f.propName] && shouldBeInserted(f.objName, obj[f.propName]));

        if (insertBefore.length > 0) {
            insertEachObject(insertBefore.map(f => ({ ...f })), obj)
                .then(() => updateMe().then(_ => thenFnc(obj)));
            let thenFnc = _ => null;
            return { then: fnc => thenFnc = fnc };
        } else return updateMe();
    }
    return { insertFnc: insertFnc, updateFnc: updateFnc };
};

export default insertUpdateFnc;