class OneupCMDs{
    constructor()
    {
        this.CMD_LIST = {};
    }

    registerCMD(cmdName, func){
        this.CMD_LIST[cmdName] = func;
    }

    get commands(){
        return this.CMD_LIST? this.CMD_LIST: {};
    }
}
module.exports = OneupCMDs;