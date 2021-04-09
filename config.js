module.exports = {
    //Rabbit
    rabbitMQConnectionString: "amqp://localhost:5672",

    //Queues
    capacityQueue: "capacityQ",
    failedQueue: "failedQ",

    //ServersListName
    serversList: 'serversNameQueue',

    // Redis
    redisServerKey: '12',

    // Timeout Values
    taskHelperTimeout: 15,
    restockHelperTimeout: 20,
}