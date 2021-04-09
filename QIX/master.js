const amqp = require('amqplib/callback_api')
const redis = require('redis')
const redisCli = redis.createClient()
const { promisify } = require("util")
const { rabbitMQConnectionString, serversList, capacityQueue, failedQueue, redisServerKey, taskHelperTimeout, restockHelperTimeout } = require('../config')

// Configura promise
const getAsync = promisify(redisCli.get).bind(redisCli)
const setAsync = promisify(redisCli.set).bind(redisCli)

// Total de Tasks: 200
// Nesta: 54
let tasks = [3, 2, 5, 6, 7, 9, 17, 5]

// Limpa a base de dados
redisCli.flushdb()

// Caso o número sorteado seja até o intervalo
let getArrayPositionByInterval = (array, number) => {
    for (let index = 0; index < array.length; index++) {
        const element = array[index]

        if (number <= element) {
            return index
        }
    }
}

// Task Watch
let taskWatchHelper = async (ch) => {

    console.log('[Task Watch Helper]')

    if (tasks.length) {
        console.log('[TWH] Há %s tarefas não finalizadas', tasks.length)
        const servers = await getAsync(redisServerKey)

        const res = await drawServersAndTasks(JSON.parse(servers), tasks)
        if (res) {
            tasks = []
            for (const tNS of res) {
                ch.sendToQueue(tNS.serverName, Buffer.from(String(tNS.task)))
            }
        }
    } else {
        console.log('[TWH] Não há tarefas não finalizadas')
        console.log('[*] Encerrando Master')
        process.exit(1)
    }
    console.log('\n')
    setTimeout(async () => {
        await taskWatchHelper(ch)
    }, taskHelperTimeout * 1000)
}

// Restaurador
let restockHelper = async (ch) => {

    console.log('[[Restock Helper]]')

    // Busca todos os servidores
    const servers = await getAsync(redisServerKey)

    let parsed = JSON.parse(servers)
    // Em ordem decrescente
    parsed.sort((a, b) => b.capacity - a.capacity) ? 1 : ((a.capacity - b.capacity) ? -1 : 0)

    // Incrementa X no último
    parsed[parsed.length - 1].capacity = Number(parsed[parsed.length - 1].capacity) + 20

    // Salva
    await setAsync(redisServerKey, JSON.stringify(parsed))

    console.log('[RHS] Servidor %s restaurado.', parsed[parsed.length - 1].serverName)
    console.log('[RHS] Capacidade: %i', parsed[parsed.length - 1].capacity)
    console.log('\n')

    // Envia flag para o worker atualizar
    ch.sendToQueue(parsed[parsed.length - 1].serverName, Buffer.from('R'))

    setTimeout(async () => {
        await restockHelper(ch)
    }, restockHelperTimeout * 1000)
}
// Caso não seja possível finalizar uma tarefa, é feito um array push
let pushTaskAndAwait = (unaccomplishedTask) => {
    console.log("[#] Capacidade negativa. Tentando novamente em X segundos")
    tasks.push(unaccomplishedTask)
}

// Sorteia os servidores e tarefas
let drawServersAndTasks = async (listS, tasks) => {

    // Organiza o array de servidores em ordem decrescente (maiores primeiro)
    listS.sort((a, b) => b.capacity - a.capacity) ? 1 : ((a.capacity - b.capacity) ? -1 : 0)
    let parsed = listS
    const array = []

    // // Se o tamanho do array dos servidores for:
    // // 1 -> só há um
    // // 2 -> só há um com capacidade menor
    // // 3 -> só há um pois arredondar-se-á para baixo => Math.floor(3/2)===1
    if (parsed.length === 1 || parsed.length === 2 || parsed.length === 3) {
        console.log("Menos de 3 servidores")

        for (const task of tasks) {
            // Organiza o array em ordem decrescente (maior primeiro)
            parsed.sort((a, b) => b.capacity - a.capacity) ? 1 : ((a.capacity - b.capacity) ? -1 : 0)

            // Se mesmo em ordem decresente, não for possível realizar a tarefa (capacidade)
            if (parsed[0].capacity - Number(task) < 0) {
                pushTaskAndAwait(task)
                return []
            }

            // Subtrai a capacidade
            parsed[0].capacity -= Number(task)

            array.push({
                serverName: parsed[0].serverName,
                task
            })
        }

        // Salva o nome e a capacidade no Redis
        await setAsync(redisServerKey, JSON.stringify(parsed))

    } else {
        console.log("Mais de 3 servidores")
        // Se for maior que 3
        // Para cada uma das tarefas, 
        // é sorteado um novo servidor
        for (const task of tasks) {

            // Obtem a metade do array (arredondada para baixo)
            // que já está ordenado em decrescente
            const halfN = parsed.slice(0, parsed.length / 2)
            // console.log('Array de n/2:')
            // console.log(halfN)

            // Array de intervalos
            const intervalsArray = []

            // Valor máximo para sorteio
            let max = 0

            // Para cada servidor,
            halfN.map(s => {
                // 1. soma o valor máximo com a capacidade do servidor atual
                max += Number(s.capacity)
                // 2. adiciona ao array de intervalos a soma
                intervalsArray.push(max)
            })

            // Sorteia um número de até o valor máximo permitido
            const drawnNumber = Math.floor((Math.random() * max))
            // console.log('Número máximo: %i', max)
            // console.log('Número sorteado: %i', drawnNumber)

            // Organiza os arrays em ordem decrescente (maiores primeiro)
            parsed.sort((a, b) => b.capacity - a.capacity) ? 1 : ((a.capacity - b.capacity) ? -1 : 0)
            halfN.sort((a, b) => b.capacity - a.capacity) ? 1 : ((a.capacity - b.capacity) ? -1 : 0)

            // console.log("Array de intervalos:")
            // console.log(intervalsArray)

            // Chama função e salva o índice
            const idx = getArrayPositionByInterval(intervalsArray, drawnNumber)

            // console.log('Intervalo %s > Sorteado %s', interval, drawnNumber)

            // Busca o servidor a partir do índice
            const foundServerIdx = parsed.findIndex(s => s.serverName == halfN[idx].serverName)

            // Se mesmo em ordem decresente, não for possível realizar a tarefa (capacidade)
            if (parsed[foundServerIdx].capacity - Number(task) < 0) {
                return pushTaskAndAwait(task)
            }

            // Subtrai capacidade
            parsed[foundServerIdx].capacity -= Number(task)

            // Salva o nome e a capacidade no Redis
            await setAsync(redisServerKey, JSON.stringify(parsed))

            console.log('\n-------------\n')
            console.log("Tarefa: %s", task)
            console.log("Servidor: %s", parsed[foundServerIdx].serverName)
            console.log('\n-------------\n')

            array.push({
                serverName: parsed[foundServerIdx].serverName,
                task
            })
        }

        // Ordena em ordem decrescente
        // Para facilitar a visualização
        array.sort((a, b) => b.task - a.task) ? 1 : ((a.task - b.task) ? -1 : 0)
        console.log("Número de Tarefas: %i X Distribuídas: %i", array.length, tasks.length)
        return array
    }
}

amqp.connect(rabbitMQConnectionString, (err, conn) => {
    if (err) {
        conn.close()
        throw err
    }
    conn.createChannel((err, ch) => {
        if (err) {
            conn.close()
            throw err
        }

        // Prefetch desativado para permitir gargalo
        // ch.prefetch(1)

        ch.purgeQueue(serversList)
        ch.purgeQueue(failedQueue)
        ch.purgeQueue(capacityQueue)

        // Recebe os nomes dos servidores
        // E cria os canais dos respectivos
        ch.assertQueue(serversList, { durable: false })

        // Recebe o nome e a tarefa falhada
        ch.assertQueue(failedQueue, { durable: false })

        // Quando um Worker enviar uma mensagem
        // no canal serversList,
        // o Master salva o Worker
        ch.consume(serversList, async (msg) => {
            const serverName = msg.content.toString()
            console.log("[*] Conexão solicitada de: %s", serverName)
            const servers = await getAsync(redisServerKey)
            if (!servers) {
                console.log('Ainda não há servidores a serem sorteados')
                return false
            }
            const res = await drawServersAndTasks(JSON.parse(servers), tasks)
            if (res) {
                tasks = []
                for (const tNS of res) {
                    ch.sendToQueue(tNS.serverName, Buffer.from(String(tNS.task)))
                }
            }

        }, { noAck: false })

        // Toda vez que for enviada uma mensagem para a CQ
        // o Master atualiza a capacidade do Worker alvo
        // Recebe a capacidade e o servidor a cada 5s
        // E toma providencias para caso algum não retorne
        ch.consume(capacityQueue, async (msg) => {
            const [serverName, capacity] = msg.content.toString().split('.')
            // snapshot.push(serverName)
            // console.log(snapshot)

            console.log('[X] Recebido @ %s:', capacityQueue)
            console.log('[X] Servidor: %s => Capacidade: %s', serverName, capacity)
            console.log('\n')

            const servers = await getAsync(redisServerKey)
            // Se houver array, atualiza
            if (servers) {
                // console.log("Há servidores")
                const parsed = JSON.parse(servers)
                const foundServerIdx = parsed.findIndex(s => s.serverName === serverName)
                if (parsed[foundServerIdx]) {
                    // console.log("Target existente")
                    parsed[foundServerIdx] = { ...parsed[foundServerIdx], capacity }
                    // Salva o nome e a capacidade
                    await setAsync(redisServerKey, JSON.stringify(parsed))
                } else {
                    // console.log("Target não existente")
                    parsed.push({
                        serverName,
                        capacity
                    })
                    // Salva o array com os nomes e capacidades
                    await setAsync(redisServerKey, JSON.stringify(parsed))
                }
                // Se não houver, cria
            } else {
                const array = [{
                    serverName,
                    capacity
                }]
                // Salva o array com o nome e a capacidade
                await setAsync(redisServerKey, JSON.stringify(array))
            }

            // Cria o canal
            // console.log("[*] Servidor conectado: %s", serverName)
            ch.assertQueue(serverName, { durable: false })

        }, { noAck: false })

        // Quando uma tarefa dá erro
        // redistribui as tarefas que não foram completas
        ch.consume(failedQueue, async (msg) => {
            const [serverName, task] = msg.content.toString().split('.')

            console.log('[#] Ocorreu um erro com uma tarefa %s do %s', task, serverName)
            console.log('\n')
            tasks.push(task)

        }, { noAck: false })

        // Busca o menor para repor X quantidades
        setTimeout(() => {
            restockHelper(ch)
        }, restockHelperTimeout * 1000)

        // Caso ainda haja tarefas na fila, é encarregado de redistribuí-las
        setTimeout(() => {
            taskWatchHelper(ch)
        }, taskHelperTimeout * 1000)

        // Primeira execução do Master
        // Distribuição de Tarefas
        setTimeout(async () => {
            console.log('[*] Iniciando distribuições de tarefa . . . ')

            // Para cada tarefa
            // Sorteia um servidor
            const servers = await getAsync(redisServerKey)
            if (!servers) {
                console.log('Ainda não há servidores a serem sorteados')
                return false
            }

            const res = await drawServersAndTasks(JSON.parse(servers), tasks)
            if (res) {
                tasks = []
                for (const tNS of res) {
                    ch.sendToQueue(tNS.serverName, Buffer.from(String(tNS.task)))
                }
            }
        }, 6 * 1000)

        // Segunda execução do Master
        // Distribuição de Tarefas
        setTimeout(async () => {
            console.log('[*] Iniciando distribuições de tarefa 2 . . . ')

            // Para cada tarefa
            // Sorteia um servidor
            const servers = await getAsync(redisServerKey)
            if (!servers) {
                console.log('Ainda não há servidores a serem sorteados')
                return false
            }
            // A quantidade total de computação nesta lista (sem erros) é de 146
            const res = await drawServersAndTasks(JSON.parse(servers), [10, 9, 15, 1, 6, 20, 4, 3, 8, 10, 60])
            if (res) {
                for (const tNS of res) {
                    ch.sendToQueue(tNS.serverName, Buffer.from(String(tNS.task)))
                }
            }
        }, 12 * 1000)

    })
})