const amqp = require('amqplib/callback_api')
const uuid = require('uuid')

const { rabbitMQConnectionString, serversList, capacityQueue, failedQueue } = require('../config')

amqp.connect(rabbitMQConnectionString, (err, conn) => {
    if (err) {
        conn.close()
        throw err
    }
    conn.createChannel((err, ch) => {

        // ch.prefetch(1)

        let capacity = 100

        const serverName = uuid.v1()
        console.log("[*] Nome do Servidor: " + serverName)

        let message = serverName + "." + capacity

        if (err) {
            ch.close()
            throw err
        }

        // Fila com os nomes do servidores
        ch.assertQueue(serversList, { durable: false })

        // Fila com o nome do servidor 
        ch.assertQueue(serverName, { durable: false })

        // Fila com a tarefa e o nome do servidor caso haja falha
        ch.assertQueue(failedQueue, { durable: false })

        // Delay 1s para enviar 
        setTimeout(() => {
            // Envia o nome do servidor para o Master
            ch.sendToQueue(serversList, Buffer.from(serverName))
        }, 1 * 1000);

        // A cada X segundos, envia a capacidade e o nome do servidor 
        // para o Master
        function send() {
            ch.sendToQueue(capacityQueue, Buffer.from(message))
            setTimeout(() => {
                send()
            }, 4 * 1000)
        }
        setTimeout(send, 4 * 1000)

        // Quando recebida uma tarefa
        // no canal de seu nome
        ch.consume(serverName, (msg) => {

            // Valor da tarefa (tempo de computação necessário)
            const val = msg.content.toString()

            // Se for recebida a flag
            if (val === 'R') {
                // Verifica se a capacidade +20 é maior ou igual a 100
                // Se sim, mantém
                if (capacity + 20 >= 100) {
                    capacity = 100
                    // Se não, adiciona mantém
                } else {
                    capacity += 20
                }
                console.log('Flag para atualização recebida')
                console.log('Capacidade total atual: %i', capacity)

            } else {
                console.log("[x] Recebida tarefa %s", val)

                // Caso a capacidade esteja negativa ou zerada
                if (!capacity || capacity - Number(val) <= 0) {
                    console.log("[#] Capacidade negativa ou zero, reenviando para o Master")

                    let concated = serverName + "." + val
                    // Enviar-se-á para o Master a tarefa e o nome do servidor
                    ch.sendToQueue(failedQueue, Buffer.from(concated))
                    console.log('[#] Ok')
                    console.log('\n')
                    message = serverName + "." + capacity.toString()

                    // Caso a capacidade ainda seja válida
                } else {

                    // Sorteia um número para simular falha
                    // 1 de X
                    const drawnNumber = Math.floor((Math.random() * 10))
                    console.log("[x] Número sorteado: %s", drawnNumber)

                    // Se for igual a X, falhará
                    // if (false) {
                    if (drawnNumber == 1) {
                        console.log("[#] Uma falha ocorrerá")
                        console.log('[#] Enviando tarefa não executada para o Master: %s', val)

                        // Envia o nome do servidor e a task para o Master redirecionar
                        let concated = serverName + "." + val
                        ch.sendToQueue(failedQueue, Buffer.from(concated))
                        console.log('[#] Ok')
                        console.log('\n')

                    } else {
                        // Subtrai da capacidade
                        capacity -= Number(val)

                        // Simula uma operação de X segundos
                        setTimeout(() => {
                            console.log('\n[.  ]Finalizado: %s', val)
                            // console.log('[.. ]Restaurando: ' + val)

                            message = serverName + "." + capacity.toString()
                            console.log('[...]Total: ' + capacity.toString())

                        }, val * 1000)
                    }
                }
            }
            // NoAck: false para que o RMQ possa apagar a mensagem
            // Uma vez que a administração de filas é manual
        }, { noAck: false })
    })
})