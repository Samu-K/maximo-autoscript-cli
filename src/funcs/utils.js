/**
 * Utility functions
 * @module utils
 */

// reading environment variables
const fs = require('fs');

/**
 * Handle command line arguments
 *
 * @returns {{}}
 *  return undefined if exit suggested
 */
function handleCliArgs(argv) {
    let args = {};
    if (argv.help !== undefined || argv.h !== undefined) {
        // help text
        const helpText = `\
        Usage: node manageAS.js <command>  [options]
        Commands:
            deploy\t\tDeploy autoscript to database
            fetch\t\tFetch autoscript from database
            diff\t\tList differences between autoscripts in database and local files
            list\t\tList all autoscripts in database
            
        Options:
            --script <name> \tSpecify single autoscript
            -a, --all\t\tDeploy all autoscripts
            
            --dry-run\t\tDry run, do not deploy to database
            -f, --force\t\tForce write when fetching autoscripts
            -h, --help\t\tPrint this help message
            -s, --silent\tSuppress all output
            -v, --verbose\tSet the verbosity level\n\t\t1= summary (default), 2= detailed
            `;
        console.log(helpText);
        return undefined;
    }

    if(argv._[0] === undefined) {
        console.log("Please give command, use -h for help");
        process.exit(1);
    }

    const silent = argv.s ?? argv.silent ?? false;
    let verbose;
    if (silent) {verbose = 0;} else {
        verbose = argv.v ?? argv.verbose ?? 1;
    }
    args.verbose = verbose;
    args.runDir = argv['runDir'];
    args.dryRun = argv['dry-run'] ?? false;
    args.write = argv.f ?? argv.force ?? false;

    if (argv._[0] === 'list') {
        return args;
    }
    const autoscriptName = argv.script;
    const allScripts = argv.all || argv.a;
    if (allScripts) {
        args.all = true;
    } else {
        if (args.script === undefined) {
            console.log("Please either provide script name with --script or use --all");
            process.exit(1);
        }
        args.script = autoscriptName.toUpperCase();
    }

    return args;
}

/**
 * Handle get request errors
 *      if verbose print whole error
 * @param error
 */
function handleDbErrors(error) {
    // if verbose just print whole error
    if (VERBOSE_ === 2) {
        console.error(error);
        process.exit(1);
    }

    if (error.message.includes('with reason "24')) {
        console.error('Error: Invalid username or password.');
        process.exit(1);
    }

    if (error.message.includes('Timeout')) {
        console.error('Error: Connection timed out.');
        process.exit(1);
    }
    // if not verbose then reduce error message
    console.error(error.message);
    process.exit(1);
}

function readConfig() {
    const confPath = './src/config.json';
    let config;
    // config is json
    // parse it
    try {
        config = JSON.parse(fs.readFileSync(confPath));
        if (config === undefined) {
            throw new Error('Config file is empty or not found');
        }
        for (let key in config) {
            process.env[key] = config[key];
        }
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
    if(config['scriptDir'] === undefined) {
        console.error('scriptDir not found in config file');
        process.exit(1);
    }
    if(config['dirLocation'] === undefined) {
        console.error('dirLocation not found in config file');
        process.exit(1);
    }

    // construct connection string inplace
    config = constructConnStr(config);
    return config;
}

/**
 * Construct connection string
 *
 * @param appConfig Application configuration
 * @returns {*} Application configuration with connection string
 */
function constructConnStr(appConfig) {
    const connValues = ['DATABASE', 'HOSTNAME', 'UID', 'PWD', 'PORT', 'PROTOCOL'];
    let connStr="";

    for (const value of connValues) {
        if (appConfig[value] === undefined) {
            console.error(`Error: ${value} not found in config file`);
            process.exit(1);
        }
        connStr += `${value}=${appConfig[value]};`;
    }
    appConfig['connstr'] = connStr;
    return appConfig;
}

/**
 * Construct SQL query
 *
 * @param method Method to use
 * @param tableName Name of the table
 * @param constraints Constraints for the query
 *      List of objects with keys col and value
 * @param values Values to append to the query
 *
 * @returns <string> SQL query
 */
function constructSql(method, tableName, constraints, values) {
    let sql = "";

    if (method === 'SELECT') {
        sql = `SELECT `;
        for (const value of values) {
            sql += `${value}, `;
        }
        // remove last comma
        sql = sql.slice(0, -2);

        sql += ` FROM ${tableName} WHERE `;

        // constrains are pairs of column and value
        for (const constraint of constraints) {
            sql += `${constraint['col']} = '${constraint['value']}' AND `;
        }
        // remove last AND
        sql = sql.slice(0, -5);
        sql += ';';
    } else if (method === "INSERT") {
        // prepare sql statement
        sql = "INSERT INTO ";
        sql += tableName + '(';
        for (let key in values) {
            sql += key + ',';
        }
        sql = sql.slice(0, -1) + ') VALUES (';
        for (let key in values) {
            sql += `'${values[key]}'` + ',';
        }
        sql = sql.slice(0, -1) + ')';
    }

    return sql;
}

module.exports = {
    handleCliArgs,
    handleDbErrors,
    readConfig,
    constructSql
}