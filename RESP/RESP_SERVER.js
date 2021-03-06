module.exports = async (settings, oneup) => {
    let RESP_SERVER = require("respress");
    let app = new RESP_SERVER();
    let _port = settings.port;
    app.listen({ port: _port }, () => {
        console.log(`RESP server is running on port ${_port}`)
    })

    app.auth((req, res) => {
        if (req.params.password == settings.password) {
            res.auth(true);
        }
        else {
            res.send(new Error("Incorrect username and/or password."))
            res.auth(false)
        }
    })

    //TODO:ADD pub sub functionality 
    //TODO:Webscoket to be created
    //TODO:Multiple topic filters per consumer 
    //TODO:RESPRESS needs to try to reconnet client to its current instance (by client id)
    app.cmd("PING", (req, res) => { res.send("PONG!") });

    //use to publish new messages, mesage counts and so on to avoid pull only 
    app.cmd("subscribe <consumerId>", (req, res) => {
        setInterval(() => { res.send("OK") }, 500);
    });

    //TODO:
    app.cmd("unsubscribe", (req, res) => { })

    app.cmd("COMMAND", (req, res) => {
        res.send(app.cmds.commandList);
    })

    app.cmd("CONSUMER <subCommand> [consumerId] [topicFilter...]", async (req, res) => {
        let selectedConsumerId = req.client.getClientVar("consumerId");
        let consumerId = req.params.consumerId;
        if (selectedConsumerId && !req.params.consumerId) {
            consumerId = selectedConsumerId;
        }
        let cmd = req.params.subCommand.toLowerCase();

        let topicFilter = req.params.topicFilter;
        switch (cmd) {
            case "create":
                if (topicFilter && consumerId) {
                    await oneup.createConsumerQ(consumerId, req.params.topicFilter);
                    res.send("OK");
                }
                else {
                    res.send(new Error("Create expects a topic filter to be sent"))
                }
                break;
            case "destroy":
                if (consumerId) {
                    await oneup.destroyConsumerQ(consumerId);
                    res.send("OK");
                }
                else {
                    res.send(new Error("Destroy expects a consumerId"))
                }
                break;
            case "get":
                if (consumerId) {
                    try {
                        let result = await oneup.qExists(consumerId);
                        res.send(result)
                    }
                    catch (error) {
                        res.send(error)
                    }
                }
                else {
                    res.send(new Error("Exists expects a consumerId"))
                }
                break;
            case "list":
                res.send(await oneup.listQs());
                break;
            case "stats":
                if (consumerId) {
                    res.send(await oneup.getQStats(consumerId));
                }
                else {
                    res.send(new Error("Stats expects a consumerId"))
                }
                break;
            case "flush.all":
                if (consumerId) {
                    res.send(await oneup.flushAll(consumerId));
                }
                else {
                    res.send(new Error("No consumerId has been provided."))
                }
                break;
            case "flush.failed":
                if (consumerId) {
                    res.send(await oneup.flushFailed(consumerId));
                }
                else {
                    res.send(new Error("No consumerId has been provided."))
                }
                break;
            case "flush.completed":
                if (consumerId) {
                    res.send(await oneup.flushCompleted(consumerId));
                }
                else {
                    res.send(new Error("No consumerId has been provided."))
                }
                break;
            default:
                res.send(new Error(`Incorrect Subcommand ${cmd}`))
                break;
        }
    })

    app.cmd("PUSH.TOPIC <message> <topic> ", async (req, res) => {
        let msg = req.params.message;
        let topic = req.params.topic;
        try {
            msg = JSON.parse(msg);
        }
        catch (e) { }
        await oneup.pushMessageTopic(topic, msg);
        res.send("OK")
    })

    app.cmd("PUSH.CONSUMER <message> [consumerId]", async (req, res) => {
        let selectedConsumerId = req.client.getClientVar("consumerId");
        let consumerId = req.params.consumerId;
        if (selectedConsumerId && !req.params.consumerId) {
            consumerId = selectedConsumerId;
        }
        if (consumerId) {
            let msg = req.params.message;
            try {
                msg = JSON.parse(msg);
            }
            catch (e) { }
            await oneup.pushMessageConsumer(consumerId, msg);
            res.send("OK")
        }
        else {
            res.send(new Error("No consumerId has been provided."))
        }

    })

    app.cmd("PULL.MESSAGE [consumerId]", async (req, res) => {
        let selectedConsumerId = req.client.getClientVar("consumerId");
        let consumerId = req.params.consumerId;
        if (selectedConsumerId && !req.params.consumerId) {
            consumerId = selectedConsumerId;
        }
        if (consumerId) {
            console.log(oneup)
            res.send(await oneup.pullMessage(consumerId));
        }
        else {
            res.send(new Error("No consumerId has been provided."))
        }
    })

    app.cmd("msg.delete [consumerId] <status> <messageKey>", async (req, res) => {
        let selectedConsumerId = req.client.getClientVar("consumerId");
        let consumerId = req.params.consumerId;
        if (selectedConsumerId && !req.params.consumerId) {
            consumerId = selectedConsumerId;
        }
        if (consumerId) {
            if (req.params.status == "failed") {
                res.send(await oneup.deleteFailed(consumerId, req.params.messageKey));
            }
            else if (req.params.status == "pending") {
                res.send(await oneup.deletePending(consumerId, req.params.messageKey));
            }
            else if (req.params.status == "completed") {
                res.send(await oneup.deleteComplete(consumerId, req.params.messageKey));
            }
            else {
                res.send(new Error("Status shoul be either failed, pending or complete."))
            }
        }
        else {
            res.send(new Error("No consumerId has been provided."))
        }
    })

    app.cmd("MSG.COMPLETE [consumerId] <messageKey>", async (req, res) => {
        let selectedConsumerId = req.client.getClientVar("consumerId");
        let consumerId = req.params.consumerId;
        if (selectedConsumerId && !req.params.consumerId) {
            consumerId = selectedConsumerId;
        }
        if (consumerId) {
            res.send(await oneup.markComplete(consumerId, req.params.messageKey));
        }
        else {
            res.send(new Error("No consumerId has been provided."))
        }
    })

    app.cmd("MSG.FAIL [consumerId] <messageKey>", async (req, res) => {
        let selectedConsumerId = req.client.getClientVar("consumerId");
        let consumerId = req.params.consumerId;
        if (selectedConsumerId && !req.params.consumerId) {
            consumerId = selectedConsumerId;
        }
        if (consumerId) {
            res.send(await oneup.markFailed(consumerId, req.params.messageKey));
        }
        else {
            res.send(new Error("No consumerId has been provided."))
        }
    })

    app.cmd("MSG.RETRY [consumerId] <messageKey>", async (req, res) => {
        let selectedConsumerId = req.client.getClientVar("consumerId");
        let consumerId = req.params.consumerId;
        if (selectedConsumerId && !req.params.consumerId) {
            consumerId = selectedConsumerId;
        }
        if (consumerId) {
            res.send(await oneup.retryFailed(consumerId, req.params.messageKey));
        }
        else {
            res.send(new Error("No consumerId has been provided."))
        }
    })

    app.cmd("SELECT <consumerId>", (req, res) => {
        if (oneup.qExists(req.params.consumerId)) {
            req.client.setClientVar("consumerId", req.params.consumerId)
            res.send(`${req.params.consumerId} has been selected.`);
        }
        else {
            res.send(new Error("ConsumerId is incorrect"))
        }
    })

    app.cmd("UNSELECT", (req, res) => {
        let selected = req.client.getClientVar("consumerId")
        req.client.delClientVar("consumerId")
        res.send(`${selected} has been unselected.`)
    })

    app.cmd("LIST [consumerId] <status> <limit> <fromKey> <reverse>", async (req, res) => {
        let selectedConsumerId = req.client.getClientVar("consumerId");
        let consumerId = req.params.consumerId;
        if (selectedConsumerId && !req.params.consumerId) {
            consumerId = selectedConsumerId;
        }
        if (consumerId) {
            res.send(await oneup.listPaged(
                consumerId,
                req.params.status,
                req.params.fromKey ? req.params.fromKey : "00",
                req.params.limit ? req.params.limit : -1,
                req.params.reverse === "true",
            ));
        }
        else {
            res.send(new Error("No consumerId has been provided."))
        }
    })
}

