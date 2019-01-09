import Node from 'eqnode';
import { hashCode, tail, matchAll, splitTextByIndices, replaceAll, mapMany } from './common';
import lift from 'eqlift';
import { checkEquals, mappingFnc, splitMultiData, mergeData, removeEmptyRows } from './sqlCommon';
import { isString } from 'util';

const queryOverFnc = sqlSelectFnc => {
  let queryOver = (retObj, definitions, name, alias, tree, arrObject, pars) => {
    let defObj = definitions.find(f => f.objName === name);
    if (defObj == null) throw new Error('could not find definition for: ' + name);
    tree = tree || new Node({ ...defObj, alias: alias || defObj.tableName, asAlias: alias || defObj.tableName, obj: retObj[name], pars: pars });
    let queryRetObj = {};

    let wheres = arrObject.wheres ? [...arrObject.wheres] : [];
    let orders = arrObject.orders ? [...arrObject.orders] : [];
    let groups = arrObject.groups ? [...arrObject.groups] : [];
    let havings = arrObject.havings ? [...arrObject.havings] : [];
    let selColumns = arrObject.selColumns ? [...arrObject.selColumns] : [];

    let cloneTree = () => tree.map(f => ({ ...f }));
    let cloneQueryOver = function (tree, obj) {
      return new queryOver(retObj, definitions, name, alias, tree, { ...arrObject, ...obj });
    }

    let replaceWith = (str, from, to) => {
      //([ ()={}]a|^a)[.]
      let matched = matchAll(str, '([ ()={]' + from + '|^' + from + ')[.]');
      let splitted = splitTextByIndices(str, matched.map(f => ({ indices: f.indices, value: f.value, text: f.value.substring(0, f.value.length - from.length - 1) + to })));
      return splitted.join('');
    }
    let replaceWithAlias = (str, from, to) => replaceWith(str, from, to + '.');

    let getSqlForProp = (prop, tree) => {
      let spl = prop.split('.');
      let defObj = tree.find(f => f.item.alias === spl[0]);
      if (defObj === undefined) throw Error('cannot find alias: ' + prop);
      let obj = {};
      tail(spl).reduce((acc, f) => { acc[f] = {}; return acc[f]; }, obj);
      let test = mappingFnc(defObj.item)(obj).filter(f => f.value != null);
      if (test.length === 0)
        throw Error('cannot find property : ' + prop);
      else if (test.length > 1)
        throw Error('cannot uniquely identify property : ' + prop);
      return spl[0] + '.' + test[0].name;
    };
    let replaceObjectProps = str => {
      let test = matchAll(str, '{.*?}');
      if (test.length > 0) {
        let vals = test.map(f => ({ ...f, text: getSqlForProp(f.value.substring(1, f.value.length - 1).trim(), tree) }));
        return vals.reduce((acc, f) => replaceAll(acc, f.value, f.text), str);
      }
      return str;
    }

    let joinsFnc = (fromObjAlias, type, objName, alias, additionalJoin, pars) => {
      let tree = cloneTree();
      let fromObj = fromObjAlias ? tree.find(f => f.item.alias === fromObjAlias) : tree;
      if (fromObj === undefined) throw new Error("could not find object: " + fromObjAlias);
      let joinObj = fromObj.item.defsObjs.find(f => f.propName === objName);
      if (joinObj === undefined) throw new Error("could not find join object: " + objName);
      let objDef = definitions.find(f => f.objName === joinObj.objName);
      if (objDef === undefined) throw new Error("could not find join object: " + joinObj.objName);
      alias = alias || objDef.tableName;

      fromObj.addChild(new Node({
        ...objDef,
        columns: objDef.columns || [],
        joinOn: lift(joinObj.joinOn).map(f => replaceWithAlias(f, 'a', fromObj.item.alias)).map(f => replaceWithAlias(f, 'b', alias)).getOrElse() + (additionalJoin ? ' and ' + additionalJoin : ''),
        propName: joinObj.propName,
        isArray: joinObj.isArray,
        type: type,
        alias: alias,
        asAlias: fromObj.item.asAlias + '_' + alias,
        obj: retObj[objName],
        pars: pars
      }));
      return cloneQueryOver(tree);
    };

    let joinFnc = type => (objName, alias, additionalJoin, pars) => joinsFnc(null, type, objName, alias, additionalJoin, pars);
    queryRetObj.join = joinFnc('join');
    queryRetObj.leftJoin = joinFnc('left join');

    let joinAliasFnc = type => (fromObjAlias, objName, alias, additionalJoin, pars) => joinsFnc(fromObjAlias, type, objName, alias, additionalJoin, pars);
    queryRetObj.join = joinFnc('join');
    queryRetObj.leftJoin = joinFnc('left join');

    queryRetObj.joinAlias = joinAliasFnc('join');
    queryRetObj.leftJoinAlias = joinAliasFnc('left join');

    queryRetObj.where = str => {
      return cloneQueryOver(cloneTree(), { wheres: [...wheres, str] });
    }

    let orderByFnc = direction => obj => {
      obj = Array.isArray(obj) ? obj : [obj];
      return cloneQueryOver(cloneTree(), { orders: [...orders, ...obj.map(f => ({ value: f, direction: direction }))] });
    }
    queryRetObj.orderByAsc = orderByFnc('asc');
    queryRetObj.orderByDesc = orderByFnc('desc');

    queryRetObj.groupBy = obj => {
      obj = Array.isArray(obj) ? obj : [obj];
      return cloneQueryOver(cloneTree(), { groups: [...groups, ...obj] });
    }
    queryRetObj.having = obj => {
      obj = Array.isArray(obj) ? obj : [obj];
      return cloneQueryOver(cloneTree(), { havings: [...havings, ...obj] });
    }

    queryRetObj.getQuery = (selectPrefix, selectSufix) => {
      tree = cloneTree(tree);
      tree.forEach(f => {
        if (f.item.pars) {
          for (var key in f.item.pars)
            f.item.tableName = replaceAll(f.item.tableName, '{' + key + '}', f.item.pars[key]);
        }
      });
      let getSelectForColumn = (def, col) => {
        if (col.name && col.value) {
          return '(' + replaceWithAlias(col.value, 'a', def.alias) + ') as ' + def.asAlias + '_' + col.name
        } else return def.alias + '.' + col.name + ' as ' + def.asAlias + '_' + col.name;
      };

      if (tree.find(f => f.item.remove) !== undefined) {
        let removed = tree.filterAny(f => f.item.remove).toArray();
        tree.forEach(f => {
          if (!f.item.remove)
            f.item.joinOn = removed.reduce((acc, r) => replaceWith(replaceObjectProps(acc), r.alias, tree.item.alias + '.' + r.asAlias + '_'), f.item.joinOn);
        })
      };

      let columns = [];
      if (selColumns.length > 0) {
        selColumns.forEach(col => {
          if (col.name) {
            let def = col.attachTo ? tree.find(f => f.item.alias === col.attachTo) : tree;
            if (def === undefined) throw new Error('attachTo not found: ' + col.attachTo);
            if (col.value)
              columns.push('(' + col.value + ') as ' + col.name);
            else if (col.attachTo)
              columns.push(getSelectForColumn(def.item, col));
            else columns.push(col.name + ' as ' + (col.alias ? col.alias : col.name));
          } else {
            if (col.endsWith('.*')) {
              let def = tree.find(f => f.item.alias === col.substring(0, col.length - 2));
              if (def === undefined) throw new Error('alias not found: ' + col);
              columns = [...columns, ...def.item.columns.map(f => getSelectForColumn(def.item, f))];
            } else {
              if (tree.find(def => {
                if (col.startsWith(def.item.alias)) {
                  let find = def.item.columns.find(g => def.item.alias + '.' + g.name === col);
                  if (find) {
                    columns.push(getSelectForColumn(def.item, find));
                    return true;
                  }
                }
                return false;
              }) === undefined) throw new Error('parameters not found: ' + col);
            }
          }
        });
      }
      else columns = tree.map(def => (!def.remove) ? def.columns.map(f => getSelectForColumn(def, f)) : []).toArray();

      return 'select ' + (selectPrefix ? selectPrefix + ' ' : '') + mapMany(columns, f => f).join(',') + (selectSufix ? ' ' + selectSufix : '')
        + '\r\nfrom ' + tree.item.tableName + ' as ' + tree.item.alias
        + (tree.children.length > 0 ? '\r\n' + tail(tree.toArray()).filter(f => !f.remove).map(f => f.type + ' ' + f.tableName + ' as ' + f.alias + ' on ' + replaceObjectProps(f.joinOn)).join('\r\n') : '')
        + (wheres.length > 0 ? '\r\nwhere ' + wheres.map(f => '(' + replaceObjectProps(f) + ')').join(' and ') : '')
        + (groups.length > 0 ? '\r\ngroup by ' + groups.map(replaceObjectProps).join(',') : '')
        + (havings.length > 0 ? '\r\nhaving ' + havings.map(f => '(' + replaceObjectProps(f) + ')').join('\r\nand') : '')
        + (orders.length > 0 ? '\r\norder by ' + orders.map(f => replaceObjectProps(f.value) + ' ' + f.direction).join(',') : '')
        ;
    }

    queryRetObj.select = columns => {
      let tree = cloneTree();
      columns.forEach(col => {
        if (isString(col)) col = { name: col };
        if (col.name) {
          let def = col.attachTo ? tree.find(f => f.item.alias === col.attachTo) : tree;
          if (def === undefined) throw new Error('attachTo not found: ' + col.attachTo);
          def.item.columns = [...def.item.columns, { ...col, alias: col.alias || col.name }];
        }
      });

      return cloneQueryOver(tree, { selColumns: [...selColumns, ...columns] });
    }
    queryRetObj.sendQuery = (selectPrefix, selectSufix) => retObj.sendQuery(queryRetObj, selectPrefix, selectSufix);
    queryRetObj.parse = (datas, leaveEmpties) => {
      let columnParser = (columns, vals, def, valsAreEmpty) => () => {
        let obj = {};
        columns.forEach((f, ind) => {
          if (vals[ind] === undefined) return;
          if (f.parseFnc) f.parseFnc(vals[ind], obj);
          else obj[f.alias] = vals[ind];
        });
        for (var key in obj)
          def.readObj.obj[key] = obj[key];

        if (!valsAreEmpty)
          def.readObj.obj.__test = {
            isModified: (compareObj) => !checkEquals(obj, compareObj),
            defObj: { ...def.item }
          };
      }
      let createObject = (obj, hash) => ({ obj: obj, $hash: hash })
      let rets = [];

      tree = tree.map(def => ({ ...def, columns: [...def.columns.map(f => ({ ...f, keyName: def.asAlias + '_' + f.name }))] }));

      datas.forEach(data => {
        tree.forEach(def => {
          let vals = def.item.columns.map(f => {
            let val = data[f.keyName];
            if (val === undefined && def.getParent == null) return data[f.name];
            return val;
          });
          let valsAreEmpty = vals.filter(f => f != null).length === 0;
          if (valsAreEmpty && !leaveEmpties) return;

          let hash = hashCode(vals.join(''));
          let parseColumns = columnParser(def.item.columns, vals, def, valsAreEmpty);

          if (def.getParent) {
            def.readObj = createObject({ ...def.item.obj }, hash);

            if (def.getParent().readObj.obj[def.item.propName]) {
              if (def.item.isArray) {
                let arr = def.getParent().readObj.obj[def.item.propName];
                if (!arr.find) throw Error(def.item.propName + ' on ' + def.getParent().item.propName + ' is not an Array!');

                let find = arr.find(f => f.$hash === hash);
                if (find == null) {
                  def.readObj = createObject({ ...tree.item.obj }, hash);
                  arr.push(def.readObj.obj);
                  parseColumns();
                } else def.readObj = find;
              } else {
                def.readObj = createObject(def.getParent().readObj.obj[def.item.propName], hash);
                parseColumns();
              }
            }
            else {
              def.getParent().readObj.obj[def.item.propName] = def.item.isArray ? [def.readObj.obj] : def.readObj.obj;
              parseColumns();
            }
          } else {
            let find = rets.find(ret => ret.$hash === hash);
            if (find == null) {
              def.readObj = createObject({ ...def.item.obj }, hash);
              rets.push(def.readObj);
              parseColumns();
            } else def.readObj = find;
          };
        });
        return tree.readObj.obj;
      });
      return rets.map(f => f.obj);
    }

    let sendTableQueries = (tableName, tableAlias, tableQuery, queries) => {
      let splitterString = ';\r\nselect 1 as test;';

      let sqls = [
        tableQuery.getQuery(null, '\r\ninto ' + tableName),
        'select * from ' + tableName + splitterString,
        ...queries.map(f => f.getQuery ? (f.getQuery(tableAlias + '.' + tableAlias + '_colid as ' + tableAlias + '_colid,') + ';' + splitterString) : f + ';\r\n'),
        'drop table ' + tableName + ';'
      ];

      sqlSelectFnc(sqls.join(';\r\n'))
        .then(response => {
          // console.log(new Date().getMilliseconds());
          let datas = splitMultiData(response.data);
          let rets = [tableQuery, ...queries.filter(f => f.getQuery)].map((query, ind) => query.parse(datas[ind], ind > 0));
          let mainData = rets[0];
          tail(rets).forEach(add => {
            mainData.forEach(dat => {
              let find = add.find(f => f.colid === dat.colid);
              if (find) mergeData(find, dat);
            });
          });
          thenFnc(mainData.map(removeEmptyRows).map(f => { delete f['colid']; return f; }));
        });
      let thenFnc = _ => null;
      return { then: fnc => thenFnc = fnc };
    }
    queryRetObj.futureTable = () => {
      let tableName = 'tbl' + Math.round(Math.random() * 1000000);
      let tableTree = cloneTree(tree);
      tableTree.item.columns = [{ name: 'colid', alias: 'colid', value: 'checksum(newid())' }, ...tableTree.item.columns];
      let tableQuery = cloneQueryOver(tableTree);

      let queries = [];

      let treeClone = tree.map(f => ({ ...f, columns: [{ name: 'colid', alias: 'colid' }, ...f.columns], remove: true }));
      treeClone.item.tableName = tableName;

      let futureObj = {
        queryTable: new queryOver(retObj, definitions, name, alias, treeClone, {}),
        sendQueries: () => {
          let consName = 'a' + Math.random();
          console.time(consName);
          sendTableQueries(tableName, treeClone.item.alias, tableQuery, queries)
            .then(response => {
              console.timeEnd(consName);
              // console.log(new Date().getMilliseconds(), response);
              console.log(response);
              window['data'] = response;
              thenFnc(response);
            });
          let thenFnc = _ => null;
          return { then: fnc => thenFnc = fnc };
        }
      }
      futureObj.add = query => {
        queries.push(query);
        return futureObj;
      };

      return futureObj;
    }

    return queryRetObj;
  }
  return queryOver;
}

export default queryOverFnc;