const fs = require("fs");
const path = require("path");
const yaml = require("yaml");
const OneUp = require("./OneUpStructs/OneUp");
const homedir = require('os').homedir();
const configPath = path.join(homedir,"oneup","configs");
const settingsPath = path.join(configPath,"settings.oneup");
fs.mkdirSync(configPath, { recursive: true });

let settingsYaml;
try{
    settingsYaml = fs.readFileSync(settingsPath,"utf-8");
}
catch(err){
    if(err.code == "ENOENT")
    {
        fs.copyFileSync('./defaults/settings.yaml', settingsPath);
        settingsYaml = fs.readFileSync(settingsPath,"utf-8");
    }
    else
    {
        console.error(err);
        process.exit(1);
    }
}


let _settings = yaml.parse(settingsYaml);
_settings.DBSettings.dbRootPath = path.resolve(_settings.DBSettings.dbRootPath); 
fs.mkdirSync(_settings.DBSettings.dbRootPath, { recursive: true });

async function main(settings){
    let oneUp = OneUp;
    await oneUp.init(settings.DBSettings.dbRootPath);

    //TODO: Create different interface capabilities "Plugins"
    let respServer = require("./RESP/RESP_SERVER")(settings.RESP, oneUp);
}

main(_settings).then(()=>{}).catch((err)=>console.log(err))
