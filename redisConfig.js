const redis = require('redis')

const redisConfig = redis.createClient()

redisConfig.on('error', (error) => console.log("REDIS ERROR: \n" + error))

redisConfig.on('connect', () => {
    console.log('REDIS CONNECTED');
});

module.exports = redisConfig