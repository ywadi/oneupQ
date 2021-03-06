//TODO: Automate msg movement use, toad-shceduler 
//TODO: DBs Manager
//TODO: Kill DB after X days if not used by consumer 
const { QMsg } = require("../DbStructs/QMsg");
const { QueueDB } = require("../DbStructs/QDB");
const level = require('level');
const Sublevel = require('level-sublevel');
const path = require("path");
let fs = require("fs").promises;
const { runInThisContext } = require("vm");
//TODO: cant create a q if created before and in db 
class OneUp {
    constructor() {
        this._dbRootPath;
        this._consumerQs;
        this._systemDb;
        this.consumersPool = {};
    }

    async init(dbRootPath) {
        //TODO: INIT ALL QUEUES 
        this._dbRootPath = dbRootPath;
        this._consumerQs = level(path.join(this._dbRootPath, "_consumerQs"), { valueEncoding: 'json' });
        this._topicFilterIndex = level(path.join(this._dbRootPath, "_topicFilter"), { valueEncoding: 'json' });
        this._systemDb = level(path.join(this._dbRootPath, "_systemDb"), { valueEncoding: 'json' });
        this.startDBs();
        return this;
    }

    async startDBs() {
        let qs = await this.listQs();
        for (let x in qs) {
            console.log("*", qs[x])
            this.consumersPool[qs[x]] = await new QueueDB().init(qs[x], this._dbRootPath);
        }
    }

    createConsumerQ(consumerId, topicFilter) {
        return new Promise((resolve, reject) => {
            let dbRootPath = this._dbRootPath;
            this._consumerQs.put(consumerId, { consumerId, topicFilter })
                .then(async function () {
                    this.consumersPool[consumerId] = await new QueueDB().init(consumerId, dbRootPath);
                    await this.createTopicIndex(topicFilter, consumerId);
                    resolve(true);
                }.bind(this))
        })

    }


    async destroyConsumerQ(consumerId) {
        try {
            if (this.consumersPool.hasOwnProperty(consumerId)) {
                fs.rmdir(path.join(this._dbRootPath, consumerId), { recursive: true });
                await this._consumerQs.del(consumerId);
                delete this.consumersPool[consumerId];
                await this.removeConsumerFromTopicIndex(consumerId)
            }
            return true;
        }
        catch (error) {
            return error;
        }
    }

    async createTopicIndex(topicFilter, consumerId) {
        console.log(topicFilter)
        for (let t in topicFilter) {
            let data;
            try {
                console.log("get", topicFilter[t])
                data = await this._topicFilterIndex.get(topicFilter[t]);
                data = data;
                console.log({ data })
                data.push(consumerId)
                await this._topicFilterIndex.put(topicFilter[t], data);
            }
            catch (err) {
                console.log(err.message)
                await this._topicFilterIndex.put(topicFilter[t], [consumerId]);
            }
        }
        return true;
    }

    async removeConsumerFromTopicIndex(consumerId) {
        new Promise((resolve, reject) => {
            this._topicFilterIndex.createReadStream()
                .on("data", async (data) => {
                    let key = data.key;
                    data = data.value;
                    const index = data.indexOf(consumerId);
                    if (index !== -1) {
                        data.splice(index, 1);
                        console.log(">",data)
                        await this._topicFilterIndex.put(key, data)
                    }
                    if (data.length === 0) {
                        await this._topicFilterIndex.del(key);
                    }
                })
                .on("end", async () => {
                    resolve();
                })
        })
    }

    consumerQsStream(options = {}) {
        return this._consumerQs.createReadStream(options);
    }

    get counsumerQsCount() {
        return new Promise((resolve, reject) => {
            let count = 0;
            this.consumerQsStream()
                .on("data", data => {
                    console.log(data)
                    ++count;
                })
                .on("end", () => {
                    resolve(count);
                })
        });
    }

    //Q Methods
    listQs() {
        return new Promise((resolve, reject) => {
            let qs = [];
            this.consumerQsStream({ values: false })
                .on("data", data => {
                    qs.push(data);
                })
                .on("end", () => {
                    resolve(qs);
                })
        });
    }

    async qExists(consumerId) {
        try {
            return await this._consumerQs.get(consumerId);
        }
        catch (error) {
            return false;
        }

    }

    async getQStats(consumerId) {
        if (await this.qExists(consumerId)) {
            return await this.consumersPool[consumerId].getMsgCountInLists();
        }
        else {
            return false;
        }

    }

    async pushMessageConsumer(consumerId, msg, topic=null) {
        //Here topic = consumer id on the call 
        if(topic)
        {msg = new QMsg(topic, msg)}
        else
        {msg = new QMsg(consumerId, msg)}
        return await this.consumersPool[consumerId].pushMessge(msg);
    }

    async pushMessageTopic(topic, msg)
    {
        let consumers;
        try{
            consumers = await this._topicFilterIndex.get(topic);
            for(let c in consumers){
                await this.pushMessageConsumer(consumers[c], msg, topic);
            }
        }
        catch(err){console.log(err);return false;}
        return true;
    }

    async pullMessage(consumerId) {
        return await this.consumersPool[consumerId].pullMessage();
    }
    async markComplete(consumerId, qmsgId) {
        return await this.consumersPool[consumerId].msgComplete(qmsgId);
    }

    async markFailed(consumerId, qmsgId) {
        return await this.consumersPool[consumerId].msgFail(qmsgId);
    }

    async retryFailed(consumerId, qmsgId) {
        return await this.consumersPool[consumerId].retryFailed(qmsgId);
    }

    async flushAll(consumerId) {
        return await this.consumersPool[consumerId].flushAll();
    }

    async deleteFailed(consumerId, qmsgId) {
        return await this.consumersPool[consumerId].deleteFailed(qmsgId);
    }

    async deletePending(consumerId, qmsgId) {
        return await this.consumersPool[consumerId].deletePending(qmsgId);
    }

    async deleteComplete(consumerId, qmsgId) {
        return await this.consumersPool[consumerId].deleteComplete(qmsgId);
    }

    async flushCompleted(consumerId) {
        return await this.consumersPool[consumerId].flushCompleted();
    }

    async reQallFailed(consumerId) {
        return await this.consumersPool[consumerId].reQallFailed();
    }

    async flushFailed(consumerId) {
        return await this.consumersPool[consumerId].flushFailed();
    }

    async listPaged(consumerId, status, fromKey, limit, reverse = false) {
        console.log(consumerId, status, fromKey, limit, reverse)
        return await this.consumersPool[consumerId].listPaged(status, fromKey, limit, reverse);
    }
}

module.exports = new OneUp();

// async function main() {
//     let qDBTest = await (new QueueDB()).init("myDB");
//     let msg = new QMsg("/1/2/3", { data: { value: 1 } });

//     await qDBTest.flushAll()
//     await qDBTest.pushMessge(msg);
//     let pmsg = await qDBTest.pullMessage();
//     // await qDBTest.msgFail(pmsg.key)
//     // await qDBTest.retryFailed(pmsg.key)

//     // let pmsg2 = await qDBTest.pullMessage();
//     // await qDBTest.deq_Active_enq_Delayed(pmsg2.key)
//     // await qDBTest.msgComplete(pmsg2.key)
// }
//main().then(() => { console.log("Main Async Done !") }).catch(err => { console.log(err) })