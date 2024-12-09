const fs = require('fs');
const path = require('path');
const ibmDb = require('ibm_db');
const {handleDbErrors, constructSql} = require('./utils');
const connStr = process.env.CONNSTR;

global.VERBOSE_ = 1;
global.DRYRUN_ = false;

/**
 * Get the max id from the table
 *
 * @param conn Database connection
 * @param tableName Name of the table
 * @returns {Promise<*>} Max id from the table
 */
async function getMaxId(conn, tableName) {
    let id;
    let idName="";
    switch(tableName) {
        case 'AUTOSCRIPT': idName = 'AUTOSCRIPTID'; break;
        case 'SCRIPTLAUNCHPOINT': idName = 'SCRIPTLAUNCHPOINTID'; break;
        case 'AUTOSCRIPTVARS': idName = 'AUTOSCRIPTVARSID'; break;
        case 'LAUNCHPOINTVARS': idName = 'LAUNCHPOINTVARSID'; break;
    }
    // get id from database
    let idStmt = `SELECT MAX(${idName}) FROM ${tableName}`;
    let result = await conn.query(idStmt).catch((err) => {
        handleDbErrors(err);
    });
    id = result[0]['1'];
    return id;
}

/**
 * Decode the object event code based on the launch point attributes
 * @param launchPoint Launch point object with event type and attributes set
 * @returns {number|null} The object event code, or null if decoding fails
 */
function decodeObjectEvent(launchPoint) {
    if (launchPoint['LAUNCHPOINTTYPE'] === "OBJECT") {
        switch (launchPoint['EVENT_TYPE']) {
            case 'init':
                return 1;
            case 'save': {
                if (launchPoint['before_save']) {
                    if (launchPoint['add'] && launchPoint['update'] && launchPoint['delete']) return 14;
                    if (launchPoint['update'] && launchPoint['delete']) return 12;
                    if (launchPoint['add'] && launchPoint['delete']) return 10;
                    if (launchPoint['delete']) return 8;
                    if (launchPoint['update'] && launchPoint['add']) return 6;
                    if (launchPoint['update']) return 4;
                    if (launchPoint['add']) return 2;
                } else if (launchPoint['after_save']) {
                    if (launchPoint['add'] && launchPoint['update'] && launchPoint['delete']) return 112;
                    if (launchPoint['update'] && launchPoint['delete']) return 96;
                    if (launchPoint['add'] && launchPoint['delete']) return 80;
                    if (launchPoint['delete']) return 64;
                    if (launchPoint['add'] && launchPoint['update']) return 48;
                    if (launchPoint['update']) return 32;
                    if (launchPoint['add']) return 16;
                } else if (launchPoint['after_commit']) {
                    if (launchPoint['add'] && launchPoint['update'] && launchPoint['delete']) return 896;
                    if (launchPoint['update'] && launchPoint['delete']) return 768;
                    if (launchPoint['add'] && launchPoint['delete']) return 640;
                    if (launchPoint['delete']) return 512;
                    if (launchPoint['add'] && launchPoint['update']) return 384;
                    if (launchPoint['update']) return 256;
                    if (launchPoint['add']) return 128;
                }
                break;
            }
            case 'validate':
                return 1024;
            case 'allow_obj_creation':
                return 2048;
            case 'allow_obj_delete':
                return 4096;
        }
    } else if (launchPoint['LAUNCHPOINTTYPE'] === "ATTRIBUTE") {
        switch (launchPoint['EVENT_TYPE']) {
            case 'validate':
                return 0;
            case 'run_action':
                return 1;
            case 'init':
                return 2;
            case 'restrict_access':
                return 8;
            case 'retrieve_list':
                return 16;
        }
    }
    return null; // Return null if decoding fails
}

/**
 * Read script from file
 *
 * @param scriptName Script name
 * @param confFolder Configuration folder
 * @param scriptFolder Script folder
 *
 * @returns {{scriptConf: {}, script: string}}
 */
function readScriptFromFile(scriptName, confFolder, scriptFolder) {
    // read script conf from file
    const scriptConfPath = path.join(confFolder, scriptName + ".json");
    if (!fs.existsSync(scriptConfPath)) {
        console.error(`Script ${scriptName} configuration file not found`);
        process.exit(1);
    }
    const scriptConf = JSON.parse(fs.readFileSync(scriptConfPath));
    const lang = scriptConf.SCRIPTLANGUAGE;
    // set extension based on language
    let ext;
    if (lang === 'js') {
        ext = '.js';
    } else if (lang === 'jython') {
        ext = '.py';
    } else {
        console.error('Unsupported language');
        process.exit(1);
    }
    const scriptPath = path.join(scriptFolder, scriptName + ext);
    if (!fs.existsSync(scriptPath)) {
        console.error("Script file not found");
        process.exit(1);
    }
    // read script file
    const script = fs.readFileSync(scriptPath, 'utf8');

    return {script, scriptConf};
}

/**
 * Check if object is new
 *
 * @param idCol Column name of id
 * @param idValue Value of id
 * @param conn Database connection
 * @param tableName Table name
 * @returns {Promise<boolean>} True if object is new, false otherwise
 */
async function isObjectNew(idCol, idValue, conn, tableName) {
    let constraits = [{
        col: idCol,
        value: idValue
    }];
    let selectValues = [
        idCol
    ];
    let sql = constructSql('SELECT', tableName, constraits, selectValues);
    let result = await conn.query(sql).catch((err) => {
        handleDbErrors(err);
    });
    return result.length === 0;
}

// scripts to check if object has been changed

async function isObjectChanged(constraints, conn, tableName, objVars) {
    let sql = constructSql('SELECT', tableName, constraints, ['*']);
    let result = await conn.query(sql).catch((err) => {
        handleDbErrors(err);
    });
    let dbObj = result[0];

    // if key contains date then skip
    for (let key in dbObj) {
        if (key.includes('DATE')) {
            continue;
        }
        if (dbObj[key] != objVars[key]) {
            if (dbObj[key] === null || objVars[key] === null || dbObj[key] === undefined || objVars[key] === undefined) {
                continue;
            }
            if(VERBOSE_ === 2) {
                if (key === 'SOURCE') {
                    console.log("Diff found: source");
                } else {
                    console.log("Diff found: ", key, dbObj[key], objVars[key]);
                }
            }
            return true;
        }
    }

    return false;
}

// scripts to update object

function updateScript(autoscriptDbVars, conn) {
    // drop create date
    delete autoscriptDbVars.CREATEDDATE;
    delete autoscriptDbVars.USERDEFINED;
    delete autoscriptDbVars.HASLD;

    // escape single quotes in source so that SQL works
    // but so that the source is still valid
    autoscriptDbVars.SOURCE = autoscriptDbVars.SOURCE.replace(/'/g, "''");
    if (autoscriptDbVars.DESCRIPTION === undefined || autoscriptDbVars.DESCRIPTION === null) {
        autoscriptDbVars.DESCRIPTION = '';
    }
    // same for description
    autoscriptDbVars.DESCRIPTION = autoscriptDbVars.DESCRIPTION.replace(/'/g, "''");

    // construct sql statement
    let sql = "UPDATE AUTOSCRIPT SET ";
    for (let key in autoscriptDbVars) {
        sql += key + ' = ' + `'${autoscriptDbVars[key]}'` + ',';
    }
    sql = sql.slice(0, -1) + ` WHERE AUTOSCRIPT = '${autoscriptDbVars.AUTOSCRIPT}'`;

    if (!DRYRUN_) {
        // execute sql statement
        if(VERBOSE_ === 2) {
            console.log(`Updating autoscript ${autoscriptDbVars.AUTOSCRIPT}...`);
        }
        conn.query(sql).catch((err) => {
            handleDbErrors(err);
        });
    }
    if (VERBOSE_ === 2) {
        console.log(sql);
    }

}

function updateScriptVar(scriptVarDbVars, conn) {
    // delete unwanted keys
    delete scriptVarDbVars.AUTOSCRIPT;

    let sql = "UPDATE AUTOSCRIPTVARS SET ";
    for (let key in scriptVarDbVars) {
        sql += key + ' = ' + `'${scriptVarDbVars[key]}'` + ',';
    }
    sql = sql.slice(0, -1) + ` WHERE VARNAME = '${scriptVarDbVars.VARNAME}'`;

    if (!DRYRUN_) {
        // execute sql statement
        if(VERBOSE_ === 2) {
            console.log(`Updating variable ${scriptVarDbVars.VARNAME}...`);
        }
        conn.query(sql).catch((err) => {
            handleDbErrors(err);
        });
    }
    if (VERBOSE_ === 2) {
        console.log(sql);
    }
}

function updateLaunchPoint(scriptLaunchPointDbVars, conn) {
    // delete unwanted keys
    delete scriptLaunchPointDbVars.AUTOSCRIPT;

    // construct sql statement
    let sql = "UPDATE SCRIPTLAUNCHPOINT SET ";
    for (let key in scriptLaunchPointDbVars) {
        // check if undefined
        if (scriptLaunchPointDbVars[key] === undefined) {
            continue;
        }
        sql += key + ' = ' + `'${scriptLaunchPointDbVars[key]}'` + ',';
    }
    sql = sql.slice(0, -1) + ` WHERE LAUNCHPOINTNAME = '${scriptLaunchPointDbVars.LAUNCHPOINTNAME}'`;

    if (!DRYRUN_) {
        // execute sql statement
        if(VERBOSE_ === 2) {
            console.log(`Updating launch point ${scriptLaunchPointDbVars.LAUNCHPOINTNAME}...`);
        }
        conn.query(sql).catch((err) => {
            handleDbErrors(err);
        });
    }
    if (VERBOSE_ === 2) {
        console.log(sql);
    }
}

function updateLpVar(launchPointVarDbVars, conn) {
    // construct sql statement
    let sql = "UPDATE LAUNCHPOINTVARS SET ";
    sql += `VARBINDINGVALUE = '${launchPointVarDbVars.VARBINDINGVALUE}'`;
    sql += ` VARNNAME = '${launchPointVarDbVars.VARNAME}'`;
    sql += ` WHERE VARNAME = '${launchPointVarDbVars.VARNAME}' AND LAUNCHPOINTNAME = '${launchPointVarDbVars.LAUNCHPOINTNAME}'`;

    if (!DRYRUN_) {
        // execute sql statement
        if (VERBOSE_ === 2) {
            console.log(`Updating launch point variable ${launchPointVarDbVars.VARNAME}...`);
        }
        conn.query(sql).catch((err) => {
            handleDbErrors(err);
        });
    }
    if (VERBOSE_ === 2) {
        console.log(sql);
    }
}

// scripts to insert object

async function insertAutoScript(conn, source, scriptConf, scriptName) {
    let id = await getMaxId(conn, 'AUTOSCRIPT');

    // current date in format yyyy-mm-dd hh:mm:ss.ssssss
    let curDate = new Date().toISOString().slice(0, 19).replace('T', ' ');
    // appends 6 zeros to the end of the date
    curDate += '.000000';

    if (scriptConf.LANGCODE === undefined) {
        scriptConf.LANGCODE = 'EN';
    }

    let autoscriptDbVars = {
        AUTOSCRIPT: scriptName,
        STATUS: 'Draft',
        DESCRIPTION: scriptConf.DESCRIPTION,
        SOURCE: source,
        CREATEDDATE: curDate,
        // VERSION: scriptConf.VERSION,
        STATUSDATE: curDate,
        CHANGEDATE: curDate,
        OWNER: 'MAXADMIN',
        CREATEDBY: 'MAXADMIN',
        CHANGEBY: 'MAXADMIN',
        HASLD: 0,
        LANGCODE: scriptConf.LANGCODE,
        SCRIPTLANGUAGE: scriptConf.SCRIPTLANGUAGE,
        USERDEFINED: '1',
        LOGLEVEL: scriptConf.LOGLEVEL,
        INTERFACE: scriptConf.INTERFACE,
        ACTIVE: scriptConf.ACTIVE
    }

    // check if script is new
    if (scriptConf.AUTOSCRIPTID !== undefined) {
        if (await isObjectNew('AUTOSCRIPTID',scriptConf.AUTOSCRIPTID,conn, 'AUTOSCRIPT') === false) {
            // check if script has been changed
            if (VERBOSE_ === 2) {
                console.log("Script exists, checking for changes...");
            }
            if(await isObjectChanged(
                [{col:'AUTOSCRIPTID',value:scriptConf.AUTOSCRIPTID}],
                conn,
                'AUTOSCRIPT',
                autoscriptDbVars
            ) === false) {

                // if not changed, skip
                if (VERBOSE_ === 2) {
                    console.log('\tScript not changed, skipping...');
                }
                return;
            }
            updateScript(autoscriptDbVars,conn);
            return;
        }
    }

    // creating new script, so increment id
    id++;
    autoscriptDbVars.AUTOSCRIPTID = id;


    // escape single quotes in source so that SQL works
    // but so that the source is still valid
    autoscriptDbVars.SOURCE = source.replace(/'/g, "''");
    // same for description
    autoscriptDbVars.DESCRIPTION = scriptConf.DESCRIPTION.replace(/'/g, "''");

    // construct sql statement
    let sql = constructSql('INSERT', 'AUTOSCRIPT', [], autoscriptDbVars);

    // execute sql statement
    if (!DRYRUN_) {
        if(VERBOSE_ === 2) {
            console.log(`Inserting autoscript ${scriptName}...`);
        }
        await conn.query(sql).catch((err) => {
            handleDbErrors(err);
        });
    }
    if (VERBOSE_ === 2) {
        console.log(sql);
    }
}

async function insertScriptVars(conn, variables, scriptName) {
    let varId = await getMaxId(conn, 'AUTOSCRIPTVARS');
    for(let variable of variables) {
        let scriptVarDbVars = {
            AUTOSCRIPT: scriptName,
            VARNAME: variable.VARNAME,
            VARBINDINGTYPE: variable.VARBINDINGTYPE,
            VARTYPE: variable.VARTYPE,
            DESCRIPTION: variable.DESCRIPTION,
            ALLOWOVERRIDE: variable.ALLOWOVERRIDE,
            ACCESSFLAG: variable.ACCESSFLAG
        }

        // check if variable is new
        if (variable.AUTOSCRIPTVARSID !== undefined) {
            if (await isObjectNew('AUTOSCRIPTVARSID',variable.AUTOSCRIPTVARSID,conn, 'AUTOSCRIPTVARS') === false) {
                if (VERBOSE_ === 2) {
                    console.log("Variable exists, checking for changes...");
                }
                // check if var changes
                if (await isObjectChanged(
                    [{col:'AUTOSCRIPTVARSID', val:variable.AUTOSCRIPTVARSID}],
                    conn,
                    'AUTOSCRIPTVARS',
                    scriptVarDbVars
                ) === false) {
                    // if not changed, skip
                    if (VERBOSE_ === 2) {
                        console.log('\tVariable not changed, skipping...');
                    }
                    continue;
                }
                // otherwise update variable
                updateScriptVar(scriptVarDbVars, conn);
                continue;
            }
        }

        // creating new variable, so increment id
        varId++;
        scriptVarDbVars.AUTOSCRIPTVARSID = varId;

        if (variable.VARBINDINGVALUE) {
            scriptVarDbVars.VARBINDINGVALUE = variable.VARBINDINGVALUE;
        }
        if (variable.LITERALDATATYPE) {
            scriptVarDbVars.LITERALDATATYPE = variable.LITERALDATATYPE;
        }
        // construct sql statement
        let sql = constructSql('INSERT', 'AUTOSCRIPTVARS', [], scriptVarDbVars);
        if (!DRYRUN_) {
            // execute sql statement
            if (VERBOSE_ === 2) {
                console.log(`Inserting variable ${variable.VARNAME}...`);
            }
            await conn.query(sql).catch((err) => {
                handleDbErrors(err);
            });
        }
        if (VERBOSE_ === 2) {
            console.log(sql);
        }
    }
}

async function insertLaunchPoints(conn, launchPoints, scriptName) {
    let lpId = await getMaxId(conn, 'SCRIPTLAUNCHPOINT');
    for (let launchPoint of launchPoints) {
        let scriptLaunchPointDbVars = {
            LAUNCHPOINTNAME: launchPoint.LAUNCHPOINTNAME,
            AUTOSCRIPT: scriptName,
            DESCRIPTION: launchPoint.DESCRIPTION,
            LAUNCHPOINTTYPE: launchPoint.LAUNCHPOINTTYPE,
            ACTIVE: launchPoint.ACTIVE
        }

        // only set object name if it exists
        // objectEvent or Attributename only exist if objectname exists
        if (launchPoint.OBJECTNAME) {
            scriptLaunchPointDbVars.OBJECTNAME = launchPoint.OBJECTNAME;
            // only set object event if it exists
            if (launchPoint.EVENT_TYPE) {
                scriptLaunchPointDbVars.OBJECTEVENT = decodeObjectEvent(launchPoint);
            }
            // only set attribute name if it exists
            if (launchPoint.ATTRIBUTENAME) {
                scriptLaunchPointDbVars.ATTRIBUTENAME = launchPoint.ATTRIBUTENAME;
            }
        }

        // only set condition if it exists
        if (launchPoint.CONDITION) {
            scriptLaunchPointDbVars.CONDITION = launchPoint.CONDITION;
        }

        // check if launchpoint is new
        if (launchPoint.SCRIPTLAUNCHPOINTID !== undefined) {
            if (await isObjectNew('SCRIPTLAUNCHPOINTID',launchPoint.SCRIPTLAUNCHPOINTID, conn, 'SCRIPTLAUNCHPOINT') === false) {
                if (VERBOSE_ === 2) {
                    console.log("Launch point exists, checking for changes...");
                }
                // check if launchpoint has been changed
                if (await isObjectChanged(
                    [{col:'SCRIPTLAUNCHPOINTID',value:launchPoint.SCRIPTLAUNCHPOINTID}],
                    conn,
                    'SCRIPTLAUNCHPOINT',
                    scriptLaunchPointDbVars
                ) === false) {
                    // if not changed, skip
                    if (VERBOSE_ === 2) {
                        console.log('\tLaunch point not changed, skipping...');
                    }
                    continue;
                }
                // othwerwise update launchpoint
                updateLaunchPoint(scriptLaunchPointDbVars, conn);
                continue;
            }
        }

        // creating new launchpoint, so increment id
        lpId++;
        scriptLaunchPointDbVars.SCRIPTLAUNCHPOINTID = lpId;

        // construct sql statement
        let sql = constructSql('INSERT', 'SCRIPTLAUNCHPOINT', [], scriptLaunchPointDbVars);

        if (!DRYRUN_) {
            // execute sql statement
            if (VERBOSE_ === 2) {
                console.log(`Inserting launch point ${launchPoint.LAUNCHPOINTNAME}...`);
            }
            await conn.query(sql).catch((err) => {
                handleDbErrors(err);
            });
        }
        if (VERBOSE_ === 2) {
            console.log(sql);
        }
    }
}

async function insertLaunchPointVars(conn, launchPoints, scriptName) {
    let lpVarId = await getMaxId(conn, 'LAUNCHPOINTVARS');

    // go through each launch point and its variables
    for (let launchPoint of launchPoints) {
        for (let variable of launchPoint.VARIABLES) {
            let launchPointVarDbVars = {
                'LAUNCHPOINTNAME': launchPoint.LAUNCHPOINTNAME,
                'AUTOSCRIPT': scriptName,
                'VARNAME': variable.VARNAME,
                'VARBINDINGVALUE': variable.VARBINDINGVALUE
            }

            if (variable.LAUNCHPOINTVARSID !== undefined) {
                // check if launch point variable is new
                if (await isObjectNew('LAUNCHPOINTVARSID', variable.LAUNCHPOINTVARSID, conn, 'LAUNCHPOINTVARS') === false) {
                    if (VERBOSE_ === 2) {
                        console.log("Launch point variable exists, checking for changes...");
                    }
                    // check if launch point variable has been changed
                    if (await isObjectChanged(
                        [{col:'VARNAME',value:variable.VARNAME},
                                    {col:'LAUNCHPOINTNAME',value:launchPointVarDbVars.LAUNCHPOINTNAME}],
                        conn,
                        'LAUNCHPOINTVARS',
                        launchPointVarDbVars
                    ) === false) {
                        if (VERBOSE_ === 2) {
                            console.log('\tLaunch point variable not changed, skipping...');
                        }
                        // if not changed, skip
                        continue;
                    }
                    // otherwise update variable
                    updateLpVar(launchPointVarDbVars, conn);
                    continue;
                }
            }

            // creating new variable, so increment id
            lpVarId++;
            launchPointVarDbVars.LAUNCHPOINTVARSID = lpVarId;

            // construct sql
            let sql = constructSql('INSERT', 'LAUNCHPOINTVARS', [], launchPointVarDbVars);

            if (!DRYRUN_) {
                // execute sql statement
                if (VERBOSE_ === 2) {
                    console.log(`Inserting launch point variable ${variable.VARNAME}...`);
                }
                await conn.query(sql).catch((err) => {
                    handleDbErrors(err);
                });
            }
            if (VERBOSE_ === 2) {
                console.log(sql);
            }
        }
    }
}

async function insertNewScript(scriptData, scriptName, conn) {
    let source = scriptData.script;
    let scriptConf = scriptData.scriptConf;

    // insert autoscript
    await insertAutoScript(conn, source, scriptConf, scriptName);

    // insert script variables
    await insertScriptVars(conn, scriptConf.variables, scriptName);

    // insert launch points
    await insertLaunchPoints(conn, scriptConf.launchPoints, scriptName);

    // insert launch point variables
    await insertLaunchPointVars(conn, scriptConf.launchPoints, scriptName);
}

// deployment scripts

async function deployAs(scriptName, scriptFolder) {
    let confFolder = path.join(scriptFolder, 'conf');
    let conn = await ibmDb.open(connStr).catch((err) => {
        handleDbErrors(err);
    });
    // construct script object
    let scriptData = readScriptFromFile(scriptName, confFolder, scriptFolder);

    // insert new script
    if (VERBOSE_ === 2) {
        console.log(`Inserting script ${scriptName}...`);
    }
    await insertNewScript(scriptData, scriptName, conn);

    conn.closeSync();
}

async function deployAllAS(scriptDir) {
    // construct path for all files in script dir
    const scriptFiles = fs.readdirSync(scriptDir);
    // call deployAs for each script
    for (let scriptFile of scriptFiles) {
        if (scriptFile === 'conf') {
            continue;
        }
        // script name can be get by slicing off the last 3 characters
        // because .py and .js are 3 characters long
        let scriptName = scriptFile.slice(0, -3);
        await deployAs(scriptName, path.join(scriptDir, 'conf'), scriptDir);
    }

}

module.exports = {
    deployAs,
    deployAllAS
}