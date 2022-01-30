var lexi = require('lexicographic-integer');
const JsonataTransform = require("./stream_filter_val");

class QueueList {
    constructor() {
        this.listName;
        this.SubQDB;
        this.lastId;
        this.lastId;
        this.QDB;
    }

    async init(QDB, listName) {
        this.listName = listName;
        this.QDB = QDB;
        this.SubQDB = await QDB.SubQDB.sublevel(this.listName);
        return this;
    }

    async flush() {
        this.SubQDB.clear();
    }

    stream_Filter(expression) {
        const transformer = new JsonataTransform(expression);
        let input = this._createReadStream({ values: true });
        let piper = input.pipe(transformer);
        return piper;
    }

    async enqMsg(qmsg, id = null) {
        let keyId = id;
        console.log(id);
        if (id == null) {
            keyId = await this.getNewId();
        }
        await this.SubQDB.put(keyId, qmsg);
        qmsg.key = keyId;
        return keyId;
    }

    async deqMsg(id = null) {
        return new Promise(async (resolve, reject) => {
            if (id == null) {
                this._createReadStream({ limit: 1 })
                    .on("data", async (data) => {
                        resolve(data);
                        await this.SubQDB.del(data.key);
                    })
                    .on("end", () => { console.log("Dequeue closed"); });
            }
            else {
                let SubQDB = this.SubQDB;
                SubQDB.get(id, async function (err, qmsg) {
                    await SubQDB.del(id);
                    resolve(qmsg);
                });
            }

        });

    }

    getNewId() {
        return this.QDB.getNewKey();
    }

    _createReadStream(streamOptions = { values: true }) {
        return this.SubQDB.createReadStream(streamOptions)
            .on('error', function (err) {
            })
            .on('close', function () {
            });
    }

    msgCount() {
        return new Promise((resolve, reject) => {
            let count = 0;
            this._createReadStream()
                .on("data", () => { ++count; })
                .on("end", () => { resolve(count); });

        });
    }
}
class PendingList extends QueueList { constructor() { super(); } }
class ActiveList extends QueueList { constructor() { super(); } }
class CompletedList extends QueueList { constructor() { super(); } }
class FailedList extends QueueList { constructor() { super(); } }
class DelayedList extends QueueList { constructor() { super(); } }

module.exports.PendingList = PendingList;
module.exports.ActiveList = ActiveList;
module.exports.CompletedList = CompletedList;
module.exports.FailedList = FailedList;
module.exports.DelayedList = DelayedList;
