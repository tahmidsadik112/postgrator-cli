const fs = require('fs');
const path = require('path');
const getUsage = require('command-line-usage');
const Postgrator = require('postgrator');
const { table, getBorderCharacters } = require('table');
const { highlight } = require('cli-highlight');
const chalk = require('chalk');
const prompts = require('prompts');
const pjson = require('./package.json');
const commandLineOptions = require('./command-line-options');

const defaultConfigFile = 'postgrator.json';

function printUsage() {
    const usage = getUsage(commandLineOptions.sections);
    console.log(usage);
}

function promiseToCallback(promise, callback) {
    promise.then(
        (data) => {
            process.nextTick(callback, null, data);
        },
        (err) => {
            process.nextTick(callback, err);
        }
    );
}

function logMessage(message) {
    // Using the system default time locale/options for now
    const messagePrefix = `[${new Date().toLocaleTimeString()}]`;
    console.log(`${messagePrefix} ${message}`);
}

function getConflictingMigrations(migrations) {
    let conflictingMigrations = [];

    migrations.forEach((migrationA) => {
        const newConflicting = migrations.filter((migrationB) => {
            return areConflictingMigrations(migrationA, migrationB);
        });
        conflictingMigrations = conflictingMigrations.concat(newConflicting);
    });

    return conflictingMigrations;
}

function areConflictingMigrations(migrationA, migrationB) {
    return (
        migrationA.action === migrationB.action && migrationA.version === migrationB.version && migrationA.filename !== migrationB.filename
    );
}

function getMigrationFileNames(migrations) {
    return migrations.map(migration => migration.filename);
}

/* -------------------------- Main ---------------------------------- */

async function run(commandLineArgs, callback) {
    if (commandLineArgs.help) {
        printUsage();
        callback(null);
        return;
    }

    if (commandLineArgs.version) {
        console.log(`Version: ${pjson.version}`);
        callback(null);
        return;
    }

    // Search for default config file if not specified
    if (!commandLineArgs.config) {
        try {
            fs.accessSync(path.join(process.cwd(), defaultConfigFile), fs.F_OK);
            commandLineArgs.config = defaultConfigFile;
        } catch (e) {
            // Default config file does not exist.
        }
    }

    if (!commandLineArgs.to && commandLineArgs.to !== 0) {
        commandLineArgs.to = 'max';
    }
    if (commandLineArgs.to !== 'max') {
        commandLineArgs.to = Number(commandLineArgs.to).toString();
    }

    let postgratorConfig;
    if (commandLineArgs.config) {
        const configFile = path.isAbsolute(commandLineArgs.config) ? commandLineArgs.config : path.join(__dirname, commandLineArgs.config);

        try {
            fs.accessSync(configFile, fs.F_OK);
        } catch (e) {
            callback(new Error(`Config file not found: ${configFile}`));
            return;
        }
        const config = require(configFile);
        postgratorConfig = {
            ...config,
            migrationDirectory: path.join(
                configFile
                    .split('/')
                    .slice(0, -1)
                    .join('/'),
                config.migrationDirectory
            ),
        };
    } else {
        postgratorConfig = {
            migrationDirectory: commandLineArgs['migration-directory'],
            driver: commandLineArgs.driver,
            host: commandLineArgs.host,
            port: commandLineArgs.port,
            database: commandLineArgs.database,
            username: commandLineArgs.username,
            password: commandLineArgs.password,
            options: { encrypt: commandLineArgs.secure || false },
        };
    }
    if (!postgratorConfig.migrationDirectory) {
        postgratorConfig.migrationDirectory = commandLineOptions.DEFAULT_MIGRATION_DIRECTORY;
    }
    if (!path.isAbsolute(postgratorConfig.migrationDirectory)) {
        postgratorConfig.migrationDirectory = path.join(process.cwd(), postgratorConfig.migrationDirectory);
    }

    if (!fs.existsSync(postgratorConfig.migrationDirectory)) {
        if (!commandLineArgs.config && commandLineArgs['migration-directory'] === commandLineOptions.DEFAULT_MIGRATION_DIRECTORY) {
            printUsage();
        }
        callback(new Error(`Directory "${postgratorConfig.migrationDirectory}" does not exist.`));
        return;
    }

    const detectVersionConflicts = postgratorConfig['detect-version-conflicts'] || commandLineArgs['detect-version-conflicts'];
    delete postgratorConfig['detect-version-conflicts']; // It's not postgrator but postgrator-cli setting

    let postgrator;
    try {
        postgrator = new Postgrator(postgratorConfig);
        if (commandLineArgs.info) {
            const tables = (await postgrator.runQuery('SHOW TABLES')).rows.map(r => Object.values(r)[0]);
            if (!tables.includes('schemaversion')) {
                console.log(`No migrations were found. Please run ${chalk.green('npm run migrate')} first`);
                process.exit(0);
            }
            const migrationsFromDb = (await postgrator.runQuery('SELECT * FROM schemaversion')).rows
                .map((m) => {
                    const newObj = {};
                    Object.keys(m).forEach((k) => {
                        newObj[k] = m[k];
                    });
                    return newObj;
                })
                .reduce((acc, current) => {
                    acc[current.version] = current;
                    return acc;
                }, {});

            const migrations = (await postgrator.getMigrations())
                .filter(m => m.action === 'do')
                .map((m) => {
                    const link = [`file://${postgratorConfig.migrationDirectory}/${m.filename}`].join('');
                    m.queryString = `${highlight(m.getSql().substr(0, 250), {
                        language: 'sql',
                        ignoreIllegals: true,
                    })}\n${link}`;
                    if (migrationsFromDb[`${m.version}`]) {
                        return {
                            ...m,
                            status:
                                m.md5 === migrationsFromDb[m.version].md5
                                    ? chalk
                                        .bgHex('#00c300')
                                        .hex('#000000')
                                        .bold('SUCCESS')
                                    : chalk
                                        .bgHex('#c91900')
                                        .hex('#FFFFFF')
                                        .bold('CORRUPTED'),
                            ranAt: migrationsFromDb[m.version].run_at,
                        };
                    }

                    return {
                        ...m,
                        status: chalk
                            .bgHex('#c7c500')
                            .hex('#000000')
                            .bold('PENDING'),
                        ranAt: 'N/A',
                    };
                });

            const tableHeaders = ['NAME', 'VERSION', 'STATUS', 'RAN_AT', 'HASH', 'SQL'];
            const tableData = [tableHeaders, ...migrations.map(m => [m.name, m.version, m.status, m.ranAt, m.md5, m.queryString])];

            const tableConfig = {
                columns: {
                    1: {
                        width: 7,
                    },
                    3: {
                        width: 15,
                    },
                    4: {
                        width: 17,
                    },
                },
                border: getBorderCharacters('norc'),
            };
            console.log(table(tableData, tableConfig));
            return;
        }
    } catch (err) {
        printUsage();
        callback(err);
        return;
    }

    postgrator.on('validation-started', migration => logMessage(`verifying checksum of migration ${migration.filename}`));
    postgrator.on('migration-started', migration => logMessage(`running ${migration.filename}`));

    let databaseVersion = null;

    const migratePromise = postgrator
        .getMigrations()
        .then((migrations) => {
            if (!migrations || !migrations.length) {
                throw new Error(`No migration files found from "${postgratorConfig.migrationDirectory}"`);
            }
            if (detectVersionConflicts) {
                const conflictingMigrations = getConflictingMigrations(migrations);
                if (conflictingMigrations && conflictingMigrations.length > 0) {
                    const conflictingMigrationFileNames = getMigrationFileNames(conflictingMigrations);
                    const conflictingMigrationFileNamesString = conflictingMigrationFileNames.join('\n');
                    throw new Error(`Conflicting migration file versions:\n${conflictingMigrationFileNamesString}`);
                }
            }
        })
        .then(() => {
            return postgrator.getDatabaseVersion().catch(() => {
                logMessage('table schemaversion does not exist - creating it.');
                return 0;
            });
        })
        .then((version) => {
            databaseVersion = version;
            logMessage(`version of database is: ${version}`);
        })
        .then(() => {
            if (commandLineArgs.to === 'max') {
                return postgrator.getMaxVersion();
            }
            return commandLineArgs.to;
        })
        .then((version) => {
            logMessage(`migrating ${version >= databaseVersion ? 'up' : 'down'} to ${version}`);
        })
        .then(() => {
            prompts({
                type: 'toggle',
                name: 'confirmation',
                message: 'Do you want to run the migrations?',
                initial: false,
                active: 'Yes',
                inactive: 'No',
            }).then((response) => {
                if (response.confirmation) {
                    console.log('A bold choice, Running the migrations.');
                    return postgrator.migrate(commandLineArgs.to);
                }
                console.log('\n A wise choice, until next time.');
                process.exit(0);
            });
        });

    promiseToCallback(migratePromise, (err, migrations) => {
        // connection is closed, or will close in the case of SQL Server
        if (err && typeof err === 'string') {
            err = new Error(err);
        }
        return callback(err, migrations);
    });
}

module.exports.run = run;
