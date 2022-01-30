class QMsg {
    constructor(topic, message) {
        this.topic = topic;
        this.message = message;
        this.key;
    }
    set id(val) { this._id = val; }

    deserialize(dataObject) {
        this._id = dataObject._id;
        this.topic = dataObject.topic;
        this.message = dataObject.message;
        this.createdAt = dataObject.createdAt;
    }
}
exports.QMsg = QMsg;
