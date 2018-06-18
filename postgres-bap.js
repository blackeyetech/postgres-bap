"use strict";

const
    DataStorePostgresBapConn = require("./postgres-bap-conn.js"),
    pg = require("pg");

// Config consts and defaults
const
    CFG_TRY_UNTIL_READY = "try-until-ready",

    CFG_DB_NAME = "dbname",
    CFG_USER = "user",
    CFG_PASSWORD = "password",
    CFG_SSL_ENABLE = "ssl-enable",
    CFG_HOST = "host",
    CFG_PORT = "port",

    CFG_TRY_UNTIL_READY_DEFAULT = true,

    CFG_SSL_ENABLE_DEFAULT = false,
    CFG_HOST_DEFAULT = "localhost",
    CFG_PORT_DEFAULT = 5432;

class DataStorePostgresBap extends besh.DataStoreBap {
    constructor(name) {
        super(name);
        
        this.tryUntilReady = this.getCfg(CFG_TRY_UNTIL_READY, 
                                         CFG_TRY_UNTIL_READY_DEFAULT);

        this.log.info(`try-until-ready set to (${this.tryUntilReady})`);

        this.log.info("Initialising postgres ...");

        this.log.info(`Using DB (${this.getRequiredCfg(CFG_DB_NAME)})`);
        this.log.info(`DB user is (${this.getRequiredCfg(CFG_USER)})`);

        this.log.info(`SSL enabled (${this.getCfg(CFG_SSL_ENABLE, 
                                                  CFG_SSL_ENABLE_DEFAULT)})`);
        this.log.info(
            `Using port (${this.getCfg(CFG_PORT, CFG_PORT_DEFAULT)})`);
        this.log.info(
            `Using host (${this.getCfg(CFG_HOST, CFG_HOST_DEFAULT)})`);

        let pgCfg = {
            database: this.getCfg(CFG_DB_NAME),
            user: this.getCfg(CFG_USER),
            password: this.getRequiredCfg(CFG_PASSWORD),
            ssl: this.getCfg(CFG_SSL_ENABLE, CFG_SSL_ENABLE_DEFAULT),
            host: this.getCfg(CFG_HOST, CFG_HOST_DEFAULT),
            port: this.getCfg(CFG_PORT, CFG_PORT_DEFAULT)
        };

        this._nextConnId = 1;
        this._pool = new pg.Pool(pgCfg);

        this._pool.on("error", (err, client) => {
            this.log.error(err);
        });

        this.log.info("Finished initialising postgres!");
    }

    isServerReady(resolve, reject) {
        // Lets check if we can query the check table 
        const
            sql = "SELECT now();";

        this._pool.query(sql).then((res) => {
            this.log.info("Postgres DB ready");
            this.log.info("Started!");
            resolve();           
        }).catch((e) => {
            let err = this.Error(e.message);

            // If we get an "ECONNREFUSED" that means the DB has not started
            if (e.code === "ECONNREFUSED" && this.tryUntilReady) {
                setTimeout(() => { 
                    this.isServerReady(resolve, reject); 
                }, 5000);
            } else {
                reject(err);
            }
        });
    }

    start() {
        this.log.info("Starting ...");

        return new Promise((resolve, reject) => {
            this.isServerReady(resolve, reject);
        });
    }

    async stop(done) {
        this.log.info("Stopping ...");
        this.log.info("Closing the pool ...");

        await this._pool.end().catch((e) => {
            throw this.Error(e.message);
        });

        this.log.info("Pool closed!");    
        this.log.info("Stopped!");
    }

    async status() {
        // Lets check if we can query the check table 
        const
            sql = "SELECT now();";

        let err;

        await this._pool.query(sql).catch((e) => {
            err = e;
            this.log.error(e);
        });

        if (err === undefined) {
            return this.statusOK;
        } else {
            return this.Status(-1, err.message);
        }
    }

    connection() {
        return new DataStorePostgresBapConn(
            this.log, this._pool, this._nextConnId++);
    }
}

// Use the same version as besh
DataStorePostgresBap.version = besh.version;

module.exports = DataStorePostgresBap;
