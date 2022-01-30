const Redis = require("ioredis");
const redis = new Redis({
    port:9000,
    password:"wadi"
}); // uses defaults unless given configuration object
redis.sendCommand(
    new Redis.Command(
        'INFO',
        [], 
        'utf-8', 
        function(err,value) {
          if (err) throw err;
          console.log(value.toString()); //-> 'OK'
        }
    )
);