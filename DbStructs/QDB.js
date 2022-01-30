var events = require('events');
var lexi = require('lexicographic-integer');
const level = require('level');
const Sublevel = require('level-sublevel');
const path = require("path");
const { ToadScheduler, SimpleIntervalJob, AsyncTask } = require('toad-scheduler');
const { PendingList, ActiveList, CompletedList, FailedList, DelayedList } = require("./QList");

class QueueDB {
    constructor() {
        this.SubQDB;
        this.QDB;
        this.lists;
        this.dbName;
        this._qdb;
        this.lastDbKey;
        const scheduler = new ToadScheduler();
        this.events = new events.EventEmitter();
        this.timers = {
            watchdogTimer: 1,
            ActiveToDelayed: 30,
            DelayedToFailed: 60,
        };
        this.events.on("operation", (action, qmsg) => {
            console.log(action, qmsg);
        });
    }

    async init(dbName, dbRootPath) {
        this.dbName = dbName;
        this.QDB = level(path.join(dbRootPath, dbName), { valueEncoding: 'json' });
        this.SubQDB = await Sublevel(this.QDB);
        this._qdb = this.SubQDB.sublevel("_qdb");
        await this._initKey();
        this.lists = {
            pending: await (new PendingList()).init(this, "Pending"),
            active: await (new ActiveList()).init(this, "Active"),
            delayed: await (new DelayedList()).init(this, "Delayed"),
            completed: await (new CompletedList()).init(this, "Completed"),
            failed: await (new FailedList()).init(this, "Failed"),
        };

        await this.startWatchdogs();
        this.events.emit("lists", "create", this.lists);
        return this;
    }

    _initKey() {
        return new Promise(async (resolve, reject) => {
            try {
                this._qdb.get("lastKey", async (err,key) => {
                    let lastKey = lexi.unpack(key, "hex");
                    if(lastKey)
                    {
                        this.lastDbKey = lastKey;
                    }
                    else
                    {
                        this.lastDbKey = 0;
                        let startKey = lexi.pack(this.lastDbKey, "hex");
                        console.log({startKey})
                        await this._qdb.put("lastKey", startKey);
                    }
                    
                    resolve();
                });
            }
            catch (err) {
                console.error(err)
            }
        })
    }

    async getNewKey() {
        let nextKey = this.lastDbKey + 1;
        this.lastDbKey = nextKey;
        nextKey = lexi.pack(nextKey, "hex");
        this._qdb.put("lastKey", nextKey);
        return nextKey;
    }

    async startWatchdogs() {
        const checkOverdues = new AsyncTask('overdue_watchdog', async () => {
            // console.log(await this.getMsgCountInLists());
            this.lists.active.stream_Filter(`$._activatedAt < ${Date.now() - (this.timers.ActiveToDelayed * 1000)}`)
                .on("data", data => {
                    this.markAsDelayed(data.key);
                });
            this.lists.delayed.stream_Filter(`$._delayedAt < ${Date.now() - (this.timers.ActiveToDelayed * 1000)}`)
                .on("data", data => {
                    this.msgFail(data.key);
                });
        });
        //TODO: Time as setting
        const msgOverdueWatchdog = new SimpleIntervalJob({ seconds: this.timers.watchdogTimer, }, checkOverdues);
        msgOverdueWatchdog.start();
    }

    async getMsgCountInLists() {
        let res = {};
        for (let l in this.lists) {
            res[l] = await this.lists[l].msgCount();
        }
        return res;
    }

    async flushAll() {
        await this.QDB.clear();
        this.events.emit("operation", "flush", {});
        return true;
    }


    async pushMessge(qmsg) {
        await this.enq_ToPending(qmsg);
    }

    async pullMessage() {
        let qmsg = await this.deq_Pending_enq_Active();
        return qmsg;
    }
    async msgComplete(qmsgId) {
        if (!await this.deq_Active_enq_Completed(qmsgId)) //try here first
            await this.deq_Delayed_enq_Completed(qmsgId);
    }

    async msgFail(qmsgId) {
        if (!await this.deq_Active_enq_Failed(qmsgId))
            await this.deq_Delayed_enq_Failed(qmsgId);
    }

    async retryFailed(qmsgId) {
        await this.deq_Failed_enq_Pending(qmsgId);
    }
    async markAsDelayed(qmsgId) {
        await this.deq_Active_enq_Delayed(qmsgId);
    }

    async deleteFailed(qmsgId) {
        return await this.lists.failed.deleteMessage(qmsgId);
    }

    async deletePending(qmsgId){
        return await this.lists.pending.deleteMessage(qmsgId);
    }

    async deleteComplete(qmsgId) {
        return await this.lists.completed.deleteMessage(qmsgId);
    }

    async flushCompleted() { 
        return await this.lists.completed.flush();
    }

    async reQallFailed() { 
        
    }

    async flushFailed() { 
        return await this.lists.failed.flush();
    }

    //TODO: offset and limit
    listPaged(status, fromKey, limit, reverse)
    {
        let list = [];
        return new Promise((resolve,reject)=>{
            this.lists[status]._createReadStream({reverse, limit, gt:fromKey})
            .on("data",(data)=>{
                list.push(data);
            })
            .on("end",()=>{
                resolve(list);
            })
        })
        
    }

    //client driven as push
    async enq_ToPending(qmsg) {
        let keyId;
        try {
            qmsg._createdAt = Date.now();
            keyId = await this.lists.pending.enqMsg(qmsg);
            this.events.emit("operation", `enq_ToPending`, { qmsg });
        }
        catch (error) { console.error(error); }
        return keyId;
    }

    //client driven as pull
    async deq_Pending_enq_Active() {
        try {
            let qmsg = await this.lists.pending.deqMsg();
            qmsg._activatedAt = Date.now();
            await this.lists.active.enqMsg(qmsg, qmsg.key);
            this.events.emit("operation", "deq_Pending_enq_Active", { qmsg });
            return qmsg;
        }
        catch (error) { console.error(error); }
    }

    //client driven by message deq confirmation
    async deq_Active_enq_Completed(qmsgId) {
        try {
            console.log(qmsgId)
            let qmsg = await this.lists.active.deqMsg(qmsgId);
            console.log(">>>", qmsg)
            if (qmsg) {
                qmsg._completedAt = Date.now();
                this.lists.completed.enqMsg(qmsg, qmsgId);
                this.events.emit("operation", "deq_Active_enq_Completed", { qmsg });
                return true;
            }
            else {
                return false;
            }
        } catch (error) {
            console.error(error);
        }
    }

    //happens if completed while in delay
    async deq_Delayed_enq_Completed(qmsgId) {
        try {
            let qmsg = await this.lists.delayed.deqMsg(qmsgId);
            qmsg._completedAt = Date.now();
            this.lists.completed.enqMsg(qmsg, qmsgId);
            this.events.emit("operation", "deq_Active_enq_Completed", { qmsg });
            return true;
        } catch (error) {
            console.error(error);
        }
    }

    //client driven if client not happy with message or failed to use it 
    async deq_Active_enq_Failed(qmsgId) {
        try {
            console.log("failed", qmsgId);
            let qmsg = await this.lists.active.deqMsg(qmsgId);
            if (qmsg) {
                console.log("?", qmsg);
                qmsg._failedAt = Date.now();
                this.lists.failed.enqMsg(qmsg, qmsgId);
                this.events.emit("operation", "deq_Active_enq_Failed", { qmsg });
                return true;
            }
            else {
                return false;
            }

        } catch (error) {
            console.error(error);
        }
    }

    //automatic, happens if in active for too long 
    //TODO: Add how long in active before moving it 
    async deq_Active_enq_Delayed(qmsgId) {
        try {
            let qmsg = await this.lists.active.deqMsg(qmsgId);
            console.log("??", qmsgId);
            qmsg._delayedAt = Date.now();
            this.lists.delayed.enqMsg(qmsg, qmsgId);
            this.events.emit("operation", "deq_Active_enq_Delayed", { qmsg });
            return true;
        } catch (error) {
            console.error(error);
        }
    }

    //Add failed back to active by automatic timmer or by user
    async deq_Failed_enq_Pending(qmsgId) {
        try {
            let qmsg = await this.lists.failed.deqMsg(qmsgId);
            qmsg._retryAt = Date.now();
            this.lists.pending.enqMsg(qmsg, qmsgId);
            this.events.emit("operation", "deq_Failed_enq_Pending", { qmsg });
            return true;
        } catch (error) {
            console.error(error);
        }
    }

    //If Delayed for too long consider failed 
    //TODO define how long before considered failed toad-scheduler
    async deq_Delayed_enq_Failed(qmsgId) {
        try {
            let qmsg = await this.lists.delayed.deqMsg(qmsgId);
            qmsg._failedAt = Date.now();
            this.lists.failed.enqMsg(qmsg, qmsgId);
            this.events.emit("operation", "deq_Delayed_enq_Failed", { qmsg });
            return true;
        } catch (error) {
            console.error(error);
        }
    }
}
exports.QueueDB = QueueDB;
