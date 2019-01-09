import queryOverFnc from './QueryOver';
import insertUpdateFnc from './InsertUpdate';
import deleteFnc from './Delete';
import { isFunction, isArray } from 'util';
import { splitMultiData } from './sqlCommon';

module.exports = (() => {
  let definitions = [];
  let retObj = {};
  retObj.define = (objName, tableName) => {
    if (definitions.find(f => f.name === objName) !== undefined) throw Error('Object is already defined: ' + objName);
    let defObj = { objName: objName, tableName: tableName, defsObjs: [], ids: [] };
    definitions.push(defObj);
    retObj[objName] = {};

    let defRetObj = {
      columns: []
    };
    defRetObj.defineColumns = (columns) => {
      if (!isArray(columns)) throw Error('columns must be defined as array on: ' + objName);
      defObj.columns = columns.map(f => f.name ? ({ ...f, alias: (f.alias ? f.alias : f.name) }) : { name: f, alias: f });
      delete defRetObj.defineColumns;
      return defRetObj;
    };
    defRetObj.defineIds = (columns) => {
      defObj.ids = columns.map(f => f.name ? { ...f } : { name: f });
      delete defRetObj.defineIds;
      return defRetObj;
    };
    let defineObjFnc = (isArray, insertBefore) => (propName, defObjName, joinOn, shouldDelete) => {
      defObj.defsObjs.push({ propName: propName, objName: defObjName, joinOn: joinOn, isArray: isArray, insertBefore: insertBefore, shouldDelete: shouldDelete });
      return defRetObj;
    }
    defRetObj.defineObj = defineObjFnc(false, false);
    defRetObj.defineObjInverse = defineObjFnc(false, true);
    defRetObj.defineObjs = defineObjFnc(true, false);

    return defRetObj;
  };

  retObj.cloneDefinition = (fromDef, toDef, tableName) => {
    var orgDef = definitions.find(f => f.objName === fromDef);
    if (orgDef === undefined) throw Error('Cannot find definition for: ' + fromDef);
    if (definitions.find(f => f.name === toDef) !== undefined) throw Error('Object is already defined: ' + toDef);
    definitions.push({ ...orgDef, objName: toDef, tableName: tableName });
  }

  retObj.queryOver = function (name, alias, pars) { return new queryOverFnc(sqlSelectFnc)(retObj, definitions, name, alias, null, { wheres: [], orders: [], groups: [], havings: [], selColumns: [] }, pars); }

  let sqlSelectFnc = _ => { throw Error('You need to set a select function!'); };
  retObj.setSelectFnc = fnc => sqlSelectFnc = fnc;

  let insUpd = insertUpdateFnc(definitions, () => sqlSelectFnc);
  retObj.insert = insUpd.insertFnc;
  retObj.update = insUpd.updateFnc;

  retObj.delete = deleteFnc(() => sqlSelectFnc);

  let sendQueries = queries => {
    sqlSelectFnc(queries.map(query => {
      if (isFunction(query.obj.getQuery))
        return query.obj.getQuery() + ';\r\nselect 1 as test;';
      else return query.obj;
    }).join('\r\n'))
      .then(response => {
        console.log(response);
        let datas = splitMultiData(response.data);
        let retData = queries.filter(query => isFunction(query.obj.getQuery))
          .map((query, ind) => ({ data: query.obj.parse(datas[ind]), name: query.name }))
          .reduce((acc, f) => { acc[f.name] = f.data; return acc; }, {});
        thenFnc(retData);
      });
    let thenFnc = _ => null;
    return { then: fnc => thenFnc = fnc };
  }
  retObj.future = () => {
    let futures = [];
    let futureObj = {};
    futureObj.add = (name, obj) => {
      futures.push({ obj: obj, name: name });
      return futureObj;
    };
    futureObj.sendQueries = () => sendQueries(futures);
    return futureObj;
  }
  retObj.sendQuery = (query, selectPrefix) => {
    sqlSelectFnc(query.getQuery(selectPrefix))
      .then(response => { thenFnc(query.parse(response)) });
    let thenFnc = _ => null;
    return { then: fnc => thenFnc = fnc };
  };

  return retObj;
})();
