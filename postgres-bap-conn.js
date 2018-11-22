"use strict";

const
    lodashMerge = require("lodash.merge");

const
    FORMAT_JSON = "json",
    FORMAT_ARRAY = "array", // array with header + data
    FORMAT_ARRAY_NO_HEADER = "array-no-header"; // array data only

class DataStorePostgresConn extends besh.DataStoreBapConn {
    constructor(log, pool, id) {
        super(log);

        this._pool = pool;
        this._client = null;
        this._id = id;

        this.log.debug("Creating connId: %d", this._id);
    }

    async create(collection, fields, id) {
        let query = {};

        let fieldStr = "",
            valuesStr = "",
            count = 1;

        query.values = [];

        for (const f in fields) {
            if (count > 1) {
                fieldStr += ",";
                valuesStr += ",";
            }

            fieldStr += f;
            query.values.push(fields[f]);
            valuesStr += `$${count}`;

            count++;
        }

        if (id === undefined) {
            query.text = 
                `INSERT INTO ${collection} (${fieldStr}) VALUES (${valuesStr})`;
        } else {
            query.text = 
                `INSERT INTO ${collection} (${fieldStr}) VALUES (${valuesStr})
                RETURNING ${id}`.replace(/\s\s+/g, " ").replace(/\n/g, " ");            
        }

        this.log.debug("connId:%d create() query: %j", this._id, query);

        let client = this._client === null ? this._pool : this._client;

        let res = await client.query(query).catch((e) => {
            // TODO: Improve error handling
            this.log.error("connId:%d '%s' happened for query (%j): %j", 
                           this._id, e, query, e);

            if (e.code === "23505") {
                throw new besh.DataStoreBapConnError(
                    "Duplicate record exists!", 
                    besh.DataStoreBapConnError.DUP_CODE, this)
            }

            throw this.Error("Something wrong with your request!", e.code);
        });

        return res.rows;
    }

    async read(collection, fields, criteria, opts) {
        // opts = { orderBy, orderByDesc, format, distinct }
        let query = {},
            defaults = {
                format: this.JSON,
                distinct: false
            };

        if (fields === undefined) {
            fields = [ "*" ];
        }

        opts = lodashMerge(defaults, opts);
        
        if (opts.distinct) {
            query.text = 
                `SELECT DISTINCT ${fields.join()} FROM ${collection}`;
        }
        else {
            query.text = `SELECT ${fields.join()} FROM ${collection}`;                
        }

        query.values = [];

        if (criteria !== undefined && Object.keys(criteria).length > 0) {
            query.text += " WHERE ";

            let position = 1;
            for (const fld in criteria) {
                if (position > 1) {
                    query.text += " AND ";
                }

                const val = criteria[fld];

                if (Array.isArray(val)) {
                    let inText = `$${position}`;
                    query.values.push(val[0]);
                    position++;

                    // Start from 1, not fom 0!
                    for (let i = 1; i < val.length; i++) {
                        inText += `,$${position}`;                                

                        query.values.push(val[i]);
                        position++;
                    }

                    query.text += `${fld} IN (${inText})`;

                } else if (typeof val === "object") {
                    query.text += `${fld}${val.op}$${position}`;
                    query.values.push(val.val);
                    position++;                        
                } else {
                    query.text += `${fld}=$${position}`;
                    query.values.push(val);
                    position++;
                }
            }
        }

        let orderByAdded = false;

        if (opts.groupBy !== undefined && opts.groupBy.length > 0) {
            query.text += ` GROUP BY ${opts.groupBy.join()}`;
        }
        if (opts.orderBy !== undefined && opts.orderBy.length > 0) {
            query.text += ` ORDER BY ${opts.orderBy.join()}`;
            query.text += " ASC"
            orderByAdded = true;
        }
        if (opts.orderByDesc !== undefined && opts.orderByDesc.length > 0) {
            if (orderByAdded) {
                query.text += `, ${opts.orderByDesc.join()} DESC`;
            } else {
                query.text += ` ORDER BY ${opts.orderByDesc.join()} DESC`;
            }
        }

        if (opts.format === this.ARRAY || 
            opts.format === this.ARRAY_NO_HEADER) {

            query.rowMode = "array";
        }

        this.log.debug("connId:%d retrieve() query: %j", this._id, query);

        let client = this._client === null ? this._pool : this._client;

        let res = await client.query(query).catch((e) => {
            // TODO: Improve error handling
            this.log.error("connId:%d '%s' happened for query (%j): %j", 
                           this._id, e, query, e);
            throw this.Error("Something wrong with your request!", e.code);
        });

        if (opts.format === this.ARRAY_HEADER) {
            return res.fields;
        }

        if (opts.format === this.ARRAY) {
            let rows = res.fields.map((f) => f.name);
            return [rows, ...res.rows];
        }
        
        return res.rows;
    }

    async update(collection, fields, criteria) {
        let query = {};

        let fieldStr = "",
            count = 1;

        query.values = [];

        for (const f in fields) {
            if (count > 1) {
                fieldStr += ",";
            }

            fieldStr += `${f}=$${count}`;
            query.values.push(fields[f]);

            count++;
        }

        query.text = `UPDATE ${collection} SET ${fieldStr}`;

        if (criteria !== undefined && 
            Object.keys(criteria).length > 0) {

            let where = ""; 
            for (const c in criteria) {
                if (where.length !== 0) {
                    where += " AND ";
                }

                where += `${c}=$${count}`;
                query.values.push(criteria[c]);
                count++;
            }

            query.text += ` WHERE ${where}`;            
        }

        this.log.debug("connId:%d update() query: %j", this._id, query);

        let client = this._client === null ? this._pool : this._client;

        let res = await client.query(query).catch((e) => {
            // TODO: Improve error handling
            this.log.error("connId:%d '%s' happened for query (%j): %j", 
                           this._id, e, query, e);
            if (e.code === "23505") {
                throw new besh.DataStoreBapConnError(
                    "Duplicate record exists!", 
                    besh.DataStoreBapConnError.DUP_CODE, this)
            }

            throw this.Error("Something wrong with your request!", e.code);
        });

        return res.rowCount;
    }

    async delete(collection, criteria) {
        let query = {};

        query.values = [];
        query.text = `DELETE FROM ${collection}`;

        let count = 1;

        if (criteria !== undefined && 
            Object.keys(criteria).length > 0) {

            let where = "";

            for (const c in criteria) {
                if (where.length !== 0) {
                    where += " AND ";
                }

                where += `${c}=$${count}`;
                query.values.push(criteria[c]);
                count++;
            }

            query.text += ` WHERE ${where}`;
        }

        this.log.debug("connId:%d delete() query: %j", this._id, query);

        let client = this._client === null ? this._pool : this._client;

        let res = await client.query(query).catch((e) => {
            // TODO: Improve error handling
            this.log.error("connId:%d '%s' happened for query (%j): %j", 
                           this._id, e, query, e);
            throw this.Error("Something wrong with your request!", e.code);
        });

        return res.rowCount;
    }

    async query(query) {
        this.log.debug("connId:%d query() query: %j", this._id, query);

        let client = this._client === null ? this._pool : this._client;

        let res = await client.query(query).catch((e) => {
            // TODO: Improve error handling
            this.log.error("connId:%d '%s' happened for query (%j): %j", 
                           this._id, e, query, e);
            throw this.Error("Something wrong with your request!", e.code);
        });
        
        return res.rows;
    }

    async exec(query) {
        this.log.debug("connId:%d query() query: %j", this._id, query);

        let client = this._client === null ? this._pool : this._client;

        let res = await client.query(query).catch((e) => {
            // TODO: Improve error handling
            this.log.error("connId:%d '%s' happened for query (%j): %j", 
                           this._id, e, query, e);
            throw this.Error("Something wrong with your request!", e.code);
        });
        
        return res.rowCount;
    }

    async connect() {
        if (this._client !== null) {
            throw this.Error(`connId:${this._id} Already have a connection!`);
        }

        this.log.debug(`connId:${this._id} Getting connection`);
        this._client = await this._pool.connect();
    }

    async release() {
        if (this._client === null) {
            throw this.Error(`connId:${this._id} Do not have a connection!`);
        }

        this.log.debug(`connId:${this._id} Releasing connection`);
        await this._client.release();
        this._client = null;            
    }

    async begin() {
        if (this._client === null) {
            throw this.Error(`connId:${this._id} Do not have a connection!`);
        }

        this.log.debug(`connId:${this._id} Beginning transaction ...`);
        await this._client.query("BEGIN;");
    }

    async commit() {
        if (this._client === null) {
            throw this.Error(`connId:${this._id} Do not have a connection!`);
        }

        this.log.debug(`connId:${this._id} Commiting transaction ...`);
        await this._client.query("COMMIT;");
    }

    async rollback() {
        if (this._client === null) {
            throw this.Error(`connId:${this._id} Do not have a connection!`);
        }

        this.log.debug(`connId:${this._id} Rolling back transaction ...`);
        await this._client.query("ROLLBACK;");
    }

    get JSON() {
        return FORMAT_JSON;
    }

    get ARRAY() {
        return FORMAT_ARRAY;
    }

    get ARRAY_NO_HEADER() {
        return FORMAT_ARRAY_NO_HEADER;
    }
}

module.exports = DataStorePostgresConn;