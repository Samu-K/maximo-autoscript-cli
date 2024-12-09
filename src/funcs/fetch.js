const fs = require('fs');
const path = require('path');
const ibmDb = require('ibm_db');
const {handleGetError, constructSql} = require('./utils.js');

/**
 * Set the event type based on the object event code
 *  Object event code decipher is in file event_codes.txt
 * @param launchPoint Launch point object
 *
 * @returns {Promise<{}>} Launch point object with event type set
 */
function encodeLaunchPointEvents(launchPoint) {
    if (launchPoint['LAUNCHPOINTTYPE'] === "OBJECT") {
        switch (launchPoint['OBJECTEVENT']) {
            case 1:
                launchPoint['EVENT_TYPE'] = 'init';
                break;
            case 2: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['add'] = true;
                launchPoint['before_save'] = true;
                break;
            }
            case 4: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['before_save'] = true;
                break;
            }
            case 6: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['add'] = true;
                launchPoint['before_save'] = true;
                break;
            }
            case 8: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['delete'] = true;
                launchPoint['before_save'] = true;
                break;
            }
            case 10: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['add'] = true;
                launchPoint['delete'] = true;
                launchPoint['before_save'] = true;
                break;
            }
            case 12: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['delete'] = true;
                launchPoint['before_save'] = true;
                break;
            }
            case 14: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['add'] = true;
                launchPoint['update'] = true;
                launchPoint['delete'] = true;
                launchPoint['before_save'] = true;
                break;
            }
            case 16: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['add'] = true;
                launchPoint['after_save'] = true;
                break;
            }
            case 32: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['after_save'] = true;
                break;
            }
            case 48: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['add'] = true;
                launchPoint['after_save'] = true;
                break;
            }
            case 64: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['delete'] = true;
                launchPoint['after_save'] = true;
                break;
            }
            case 80: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['add'] = true;
                launchPoint['delete'] = true;
                launchPoint['after_save'] = true;
                break;
            }
            case 96: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['delete'] = true;
                launchPoint['after_save'] = true;
                break;
            }
            case 112: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['add'] = true;
                launchPoint['delete'] = true;
                launchPoint['after_save'] = true;
                break;
            }
            case 128: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['add'] = true;
                launchPoint['after_commit'] = true;
                break;
            }
            case 256: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['after_commit'] = true;
                break;
            }
            case 384: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['add'] = true;
                launchPoint['after_commit'] = true;
                break;
            }
            case 512: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['delete'] = true;
                launchPoint['after_commit'] = true;
                break;
            }
            case 640: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['add'] = true;
                launchPoint['delete'] = true;
                launchPoint['after_commit'] = true;
                break;
            }
            case 768: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['delete'] = true;
                launchPoint['after_commit'] = true;
                break;
            }
            case 896: {
                launchPoint['EVENT_TYPE'] = 'save';
                launchPoint['update'] = true;
                launchPoint['add'] = true;
                launchPoint['delete'] = true;
                launchPoint['after_commit'] = true;
                break;
            }
            case 1024: {
                launchPoint['EVENT_TYPE'] = 'validate';
                break;
            }
            case 2048: {
                launchPoint['EVENT_TYPE'] = 'allow_obj_creation';
                break;
            }
            case 4096: {
                launchPoint['EVENT_TYPE'] = 'allow_obj_delete';
                break;
            }
        }
    } else if (launchPoint['LAUNCHPOINTTYPE'] === "ATTRIBUTE") {
        switch (launchPoint['OBJECTEVENT']) {
            case 0:
                launchPoint['EVENT_TYPE'] = 'validate';
                break;
            case 1:
                launchPoint['EVENT_TYPE'] = 'run_action';
                break;
            case 2:
                launchPoint['EVENT_TYPE'] = 'init';
                break;
            case 8:
                launchPoint['EVENT_TYPE'] = 'restrict_access';
                break;
            case 16:
                launchPoint['EVENT_TYPE'] = 'retrieve_list';
                break;
        }
    }
    // remove object event from launch point
    delete launchPoint['OBJECTEVENT']

    return launchPoint;
}

async function readScript(scriptData, conn) {
    const script = {};
    script.name = scriptData['AUTOSCRIPT'];
    script.source = scriptData['SOURCE'];
    script.config = await formatScriptConfig(scriptData, conn);

    return script;
}

/**
 * Get the names of the scripts in the database
 *

 * @returns {Promise<*[]>} List of script names
 */
async function getScriptNames() {
    let conn = await ibmDb.open(CONNSTR).catch(error => {
        handleGetError(error);
    });
    let stmnt = await conn.prepare("SELECT AUTOSCRIPT FROM AUTOSCRIPT;");
    let result = await stmnt.execute();
    let data = await result.fetchAll();
    result.close();
    stmnt.close();

    let scriptNames = [];
    for (const row of data) {
        scriptNames.push(row['AUTOSCRIPT']);
    }

    return scriptNames;
}

/**
 * Get the values from the database
 *
 * @param dbColumnNames List of column names
 * @param conn Database connection
 * @param tableName Name of the table
 * @param constraints Constraints for the query
 * @param selectValues Values to select
 *      Default is all values
 * @returns {Promise<[]>} List of values
 */
async function getValuesFromDatabase(dbColumnNames, conn, tableName, constraints, selectValues=['*']) {
    // construct sql query
    let sql = constructSql('SELECT',tableName, constraints, selectValues);
    let stmnt = await conn.prepare(sql);
    let result = await stmnt.execute();
    let queriedValues = await result.fetchAll();
    result.close();
    stmnt.close();

    let launchPointVarColumns = [];

    if (tableName === 'SCRIPTLAUNCHPOINT') {
        // set variables to launchpoint
        launchPointVarColumns = [
            'VARNAME',
            'VARBINDINGVALUE',
            'LAUNCHPOINTVARSID'
        ]
    }

    // create list of values to return
    let values = [];
    // go through each queried value
    for (const queriedValue of queriedValues) {
        let value = {};

        // set values to object
        for (const column of dbColumnNames) {
            if (queriedValue !== null) {
                value[column] = queriedValue[column];
            }
        }

        // if launchpoint we need extra values
        if (tableName === 'SCRIPTLAUNCHPOINT') {
            value = await handleLaunchPointVars(value, launchPointVarColumns, conn);
        }

        values.push(value);
    }

    return values;
}

/**
 * Get launch point variables
 *
 * @param launchPoint launchpoint object
 * @param launchPointVarColumns columns to get from database
 * @param conn database connection
 * @returns {Promise<{}>} launchpoint object with variables added
 */
async function handleLaunchPointVars(launchPoint, launchPointVarColumns, conn) {
    let launchPointVarConstraints = [
        {col:'AUTOSCRIPT',value: launchPoint['AUTOSCRIPT']},
        {col:'LAUNCHPOINTNAME',value: launchPoint['LAUNCHPOINTNAME']}
    ]

    launchPoint['VARIABLES'] = await getValuesFromDatabase(
        launchPointVarColumns,
        conn,
        'LAUNCHPOINTVARS',
        launchPointVarConstraints
    );

    // set launch point events based on code of object event
    if (launchPoint['OBJECTEVENT'] !== undefined) {
        launchPoint = encodeLaunchPointEvents(launchPoint);
    }

    return launchPoint;
}

/**
 * Get the code and configuration for each script
 *
 * @returns {Promise<*[]>} List of script objects
 */
async function readScriptsFromDb() {
    // list of scripts
    let conn = await ibmDb.open(CONNSTR).catch(error => {
        handleGetError(error);
    });
    let stmnt = await conn.prepare("SELECT * FROM AUTOSCRIPT;");
    let result = await stmnt.execute();
    let data = await result.fetchAll();

    let scripts = [];
    for (const row of data) {
        scripts.push(await readScript(row, conn));
    }
    result.close();
    stmnt.close();
    conn.close();

    return scripts;
}

/**
 * Write scripts to the output directory
 *
 * @param scripts List of script objects to write
 * @param outputDir Output directory
 * @param force_write Force write the scripts to the output directory
 *
 * @returns void
 */
async function writeScripts(scripts, outputDir, force_write) {
    let updated = 0;
    let created = 0;
    let total = 0;

    // construct paths and check if they exist
    const scriptsDir = path.resolve(outputDir);
    const confDir = path.resolve(outputDir, 'conf');
    checkPaths(scriptsDir, confDir);

    if (VERBOSE_ === 2) {console.log(`Writing scripts to ${scriptsDir}`);}
    for (const script of scripts) {
        total++;
        let result = await writeScript(script, scriptsDir, confDir, force_write);
        if ( result === 1) {
            updated++;
        } else if (result === 2) {
            created++;
        }
    }

    if (VERBOSE_ === 2) {
        console.log("----------------------------------------------------");
    }
    if (VERBOSE_ > 0) {
        console.log(`Total scripts: ${total}, Updated: ${updated}, Created: ${created}`);
    }
}

function checkPaths(scriptsDir, confDir) {
    // Ensure the output directory exists
    if (!fs.existsSync(scriptsDir)) {
        if (VERBOSE_ === 2) {console.log(`No script directory found, creating one at ${scriptsDir}`);}
        fs.mkdirSync(scriptsDir);
    }
    // check if config folder exists
    if (!fs.existsSync(confDir)) {
        if (VERBOSE_ === 2) {console.log(`No config directory found, creating one at ${confDir}`);}
        fs.mkdirSync(confDir);
    }
}

/**
 * Write the script to the output directory
 *
 * @param script Script object
 * @param scriptsDir Output directory for scripts
 * @param confDir Output directory for config files
 * @param force_write Force write the script to the output directory
 * @returns {Promise<number>}
 *     0 - No changes
 *     1 - Script was updated
 *     2 - Script was created
 */
async function writeScript(script, scriptsDir, confDir, force_write) {
    const name = script.name;
    const ext = script.config.SCRIPTLANGUAGE.toLowerCase() === 'jython' ? '.py' : '.js';
    const filePath = path.join(scriptsDir, `${name}${ext}`);
    const confPath = path.join(confDir, `${name}.json`);

    // Check if the files exist
    // and not forced write
    if (fs.existsSync(filePath) && fs.existsSync(confPath) && !force_write) {
        if (updateIfRequired(script, filePath, confPath)) {
            return 1;
        }
    } else {
        writeScriptToFile(script, filePath, confPath);
        return 2;
    }
    return 0;
}

/**
 * Write the configuration for the script
 *
 * @param data Script configuration data
 * @param conn Database
 *
 * @returns {Promise<{}>} Configuration object
 */
async function formatScriptConfig(data, conn) {
    const config = {};
    const asName = data['AUTOSCRIPT'];

    const configValues = [
        'DESCRIPTION',
        'VERSION',
        'ACTIVE',
        'LOGLEVEL',
        'INTERFACE',
        'AUTOSCRIPTID',
        'LANGCODE',
        'SCRIPTLANGUAGE'
    ]
    // get values already in row
    if (data) {
        // get config values and set to object
        for (const key of configValues) {
            if (data[key] !== undefined) {
                config[key] = data[key];
            }
        }
    }

    // get variables
    let variableValues = [
        'VARNAME',
        'DESCRIPTION',
        'VARBINDINGTYPE',
        'VARTYPE',
        'ALLOWOVERRIDE',
        'ACCESSFLAG',
        'LITERALDATATYPE',
        'AUTOSCRIPTVARSID'
    ]
    config.variables = await getValuesFromDatabase(
        variableValues,
        conn,
        'AUTOSCRIPTVARS',
        [{col:'AUTOSCRIPT',value: asName}]
    );


    // get launch points
    const launchPointValues = [
        'LAUNCHPOINTNAME',
        'LAUNCHPOINTTYPE',
        'ACTIVE',
        'DESCRIPTION',
        'OBJECTNAME',
        'OBJECTEVENT',
        'SCRIPTLAUNCHPOINTID'
    ]
    config.launchPoints = await getValuesFromDatabase(
        launchPointValues,
        conn,
        'SCRIPTLAUNCHPOINT',
        [{col:'AUTOSCRIPT',value: asName}]
    );

    return config;
}

/**
 * Update the script and config files if required
 *
 * @param script Script object
 * @param filePath Path to the script file
 * @param confPath Path to the config file
 * @returns {boolean} True if the script or config was updated
 */
function updateIfRequired(script, filePath, confPath) {
    const existingData = fs.readFileSync(filePath, 'utf-8');
    const existingConf = fs.readFileSync(confPath, 'utf-8');
    const name = script.name;
    let isUpdated = false;

    // check if code has been updated
    if (existingData === script.source) {
        if (VERBOSE_ === 2) {console.log(`No changes for ${name}.`);}
    } else {
        if (VERBOSE_ === 2) {console.log(`Updating ${name} due to changes.`);}
        isUpdated = true;
        fs.writeFileSync(filePath, script.source, 'utf-8');
    }

    // check if config has been updated
    if (existingConf === JSON.stringify(script.config, null, 4)) {
        if (VERBOSE_ === 2) {console.log(`No changes for ${name}.json`);}
    } else {
        if (VERBOSE_ === 2) {console.log(`Updating ${name} config due to changes.`);}
        isUpdated = true;
        fs.writeFileSync(confPath, JSON.stringify(script.config, null, 4), 'utf-8');
    }
    return isUpdated;
}

// write new script and config to disc
function writeScriptToFile(script, filePath, confPath) {
    if (VERBOSE_ === 2) {console.log(`Creating new script: ${script.name}.`);}
    fs.writeFileSync(filePath, script.source, 'utf-8');
    fs.writeFileSync(confPath, JSON.stringify(script.config, null, 4), 'utf-8');
}

async function fetchAs(scriptName, forceWrite, scriptDir) {
    if (VERBOSE_ > 1) {console.log(`Extracting script ${scriptName}`)}
    let conn = await ibmDb.open(connStr).catch(error => {
        handleGetError(error);
    });
    let stmnt = await conn.prepare(`SELECT * FROM AUTOSCRIPT WHERE AUTOSCRIPT='${scriptName}';`);
    let result = await stmnt.execute();
    let data = await result.fetchAll();
    result.close();
    stmnt.close();
    const script = await readScript(data[0], conn);
    await writeScript(script, scriptDir, path.join(scriptDir, 'conf'), forceWrite);
    if (VERBOSE_ > 0) {console.log(`Script ${scriptName} extracted.`)}
}

async function fetchAllAs(scriptDir, forceWrite) {
    if (VERBOSE_ > 1) {console.log('Extracting scripts...');}
    const scripts = await readScriptsFromDb();
    await writeScripts(scripts, scriptDir, forceWrite);
    if (VERBOSE_ > 0) {console.log('Scripts extraction complete.');}
}

async function diffAs(script, scriptFolderPath) {
    let modified = false;

    // get script from disk
    const scriptConfPath = path.resolve(scriptFolderPath, 'conf', `${script}.json`);
    const scriptConf = JSON.parse(fs.readFileSync(scriptConfPath, 'utf-8'));
    const scriptPath = path.resolve(scriptFolderPath, `${script}.${scriptConf.SCRIPTLANGUAGE.toLowerCase() === 'jython' ? 'py' : 'js'}`);
    const scriptData = fs.readFileSync(scriptPath, 'utf-8');
    // get script from db
    let conn = await ibmDb.open(CONNSTR).catch(error => {
        handleGetError(error);
    });
    let stmnt = await conn.prepare(`SELECT * FROM AUTOSCRIPT WHERE AUTOSCRIPT='${script}';`);
    let result = await stmnt.execute();
    let data = await result.fetchAll();
    result.close();
    stmnt.close();
    const dbScript = await readScript(data[0], conn);
    // get conf from db
    const dbConf = await formatScriptConfig(data[0], conn);
    // compare and print differences
    if (scriptData !== dbScript.source) {
        if (VERBOSE_ > 0) {console.log(`Script ${script} has been modified.`);}
        if (VERBOSE_ === 2) {
            // go row by row and print difference git-style
            const scriptDataLines = scriptData.split('\n');
            const dbScriptLines = dbScript.source.split('\n');
            for (let i = 0; i < scriptDataLines.length; i++) {
                if (scriptDataLines[i] !== dbScriptLines[i]) {
                    console.log(`- ${scriptDataLines[i]}`);
                    console.log(`+ ${dbScriptLines[i]}`);
                }
            }
        }
        modified = true;
    }
    if (JSON.stringify(scriptConf) !== JSON.stringify(dbConf)) {
        if (VERBOSE_ > 0) {console.log(`Configuration for script ${script} has been modified.`)}
        modified = true;
    }
    return modified;
}

async function diffAllAs(scriptFolderPath) {
    const scripts = await readScriptsFromDb();
    let modifiedCount = 0;
    for (const script of scripts) {
        if (await diffAs(script.name, scriptFolderPath)) {
            modifiedCount++;
        }
    }
    return modifiedCount;
}

module.exports = {
    fetchAs,
    fetchAllAs,
    getScriptNames,
    diffAs,
    diffAllAs
}