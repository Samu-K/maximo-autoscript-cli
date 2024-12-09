/** A script to deploy or fetch Maximo automation scripts.
 *  From the database
 *
 *  For commands use --help
 * */

const deploy = require('./deploy.js');
const fetch = require('./fetch.js');
const utils = require('./utils.js');

// Set DB2 codepage to UTF-8
process.env.DB2CODEPAGE = "1208";

const argv = require('minimist')(process.argv.slice(2));

async function main() {
    const appConfig = utils.readConfig();
    const scriptFolderPath = appConfig['scriptDir'];
    global.CONNSTR = appConfig['connstr'];

    const args = utils.handleCliArgs(argv);
    if (args === undefined) {
        process.exit(0);
    }
    global.VERBOSE_ = args.verbose;
    global.DRYRUN_ = args.dryRun;

    if (argv._.length === 0) {
        console.error('No command specified');
        process.exit(1);
    }

    const command = argv._[0];
    if(args.all === undefined) {
        let script = args.script;
        if (command === 'deploy') {
            await deploy.deployAs(script,scriptFolderPath);
            if (VERBOSE_ === 2) {
                console.log("----------------------------------------------------");
            }
            if (VERBOSE_ > 0) {
                console.log(`${script} deployed successfully`);
            }
            if (VERBOSE_ === 2) {
                console.log("----------------------------------------------------");
            }
        }
        if (command === 'fetch') {
            await fetch.fetchAs(script, scriptFolderPath, args.write);

        }

        if (command === 'diff') {
            let mod = await fetch.diffAs(script, scriptFolderPath);
            if(!mod) {
                console.log(`${script} is up to date`);
            }
        }
    } else {
        if (command === 'deploy') {
            await deploy.deployAllAS();
            if (VERBOSE_ === 2) {
                console.log("----------------------------------------------------");
            }
            if (VERBOSE_ > 0) {
                console.log(`Scripts deployed successfully`);
            }
            if (VERBOSE_ === 2) {
                console.log("----------------------------------------------------");
            }
        }
        if (command === 'fetch') {
            await fetch.fetchAllAs(scriptFolderPath,args.write);
        }
        if (command === 'diff') {
            let modCount = await fetch.diffAllAs(scriptFolderPath);
            if (VERBOSE_ > 0) {
                if (modCount === 0) {
                    console.log(`All scripts are up to date`);
                } else {
                    console.log(`${modCount} scripts have been modified`);
                }
            }
        }
    }

    if (command === 'list') {
        const scriptNames = await fetch.getScriptNames();
        for(const script of scriptNames) {
            console.log(script);
        }
    }
}

main();