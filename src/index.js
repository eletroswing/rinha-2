const { Pool } = require('pg');
const cluster = require('cluster');

const ws = require("ws");
const dotenv = require('dotenv');

dotenv.config();

const TCPServer = require('./tcp.js');

let cacheConnect;

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
});

class Cache {
    constructor() {
        this.cache = {};

        cacheConnect.on('message', (data) => {
            const res = JSON.parse(data.toString());
            this.#process(res)
        });
    }

    #addToQueue(data) {
        const handleGet = () => {
            return { value: this.cache[data.key] };
        };

        const handlePut = () => {
            this.cache[data.key] = data.value;
            return { value: this.cache[data.key] };
        };

        const handleTransaction = () => {
            let dt = this.cache[data.key];
            const { saldo, ultimas_transacoes } = dt;
            const { valor, tipo, descricao } = data.value;

            if (tipo === 'd') {
                if (saldo.limite < ((saldo.total - valor) * -1)) {
                    return { value: { error: { type: tipo, error: `no-limit` } } };
                }
                saldo.total -= valor;
            }

            if (tipo === 'c') {
                saldo.total += valor;
            }

            let transactions = ultimas_transacoes ? ultimas_transacoes.slice(0, 9) : [];
            let latest_transactions = [{
                valor: valor,
                tipo: tipo,
                descricao: descricao,
                realizada_em: new Date().toISOString(),
            }, ...transactions];

            this.cache[data.key] = {
                saldo,
                ultimas_transacoes: latest_transactions,
            };

            return {
                value: {
                    saldo,
                    ultimas_transacoes: latest_transactions,
                }
            };
        };

        switch (data.type) {
            case 'get':
                return handleGet();
            case 'put':
                return handlePut();
            case 'transaction':
                return handleTransaction();
            default:
                return { error: 'Invalid operation type' };
        }
    };

    #process(data) {
        if (data.queue == `clear`) {
            this.cache = {};
            RunCacheSeed()
            return
        }

        if (data.queue == `addToQueue`) {
            const info = data.data;
            const res = this.#addToQueue(info);
            return res.value
        };
    }

    _emit(queue, data) {
        cacheConnect.send(JSON.stringify({
            queue,
            data
        }))

        return
    }

    async clear() {
        this._emit(`clear`)
    }

    async execute(data) {
        this._emit(`addToQueue`, data)

        return this.#process({ queue: `addToQueue`, data })
    }

}

let CacheLib;

const server = new TCPServer();

server.on("connection", async (socket, data) => {
    const [headers, body] = data.split("\r\n\r\n");
    const firstLine = headers.split("\r\n")[0];
    const [method, url, httpVersion] = firstLine.split(" ");

    if (url) {
        const split_url = url.split('/');
        split_url.shift();

        if (method === 'POST' && split_url[0] === 'admin' && split_url[1] === 'reset') {
            return handlePostAdminDbReset(socket);
        }

        else if (method === 'GET' && split_url[0] === 'clientes' && split_url[2] === 'extrato') {
            //por algum motivo, declarar dessa forma ao invés de fluir o socket atráves de uma função é mais performatico em cerca de 1 ms
            if (split_url[1]) {
                if (!split_url[1] || isNaN(split_url[1])) {
                    const content = `Id precisa ser um número`;
                    const type = `Content-Type: text/plain\r\nContent-Length: ${content.length + 1}`
                    const response = `HTTP/1.1 422 UNPROCESSABLE ENTITY\r\n${type}\r\n\r\n${content}`;
                    socket.write(response);
                    return
                }

                if (split_url[1] < 1 || split_url[1] > 5) {
                    const content = `Cliente não existe`;
                    const type = `Content-Type: text/plain\r\nContent-Length: ${content.length + 1}`
                    const response = `HTTP/1.1 404 NOT FOUND\r\n${type}\r\n\r\n${content}`;
                    socket.write(response);
                    return
                }
                handleGetClientes(socket, split_url[1]);
                return
            }
        }
        else if (method === 'POST' && split_url[0] === 'clientes' && split_url[2] === 'transacoes') {
            if (split_url[1]) {
                if (!split_url[1] || isNaN(split_url[1])) {
                    const content = `Id precisa ser um número`;
                    const type = `Content-Type: text/plain\r\nContent-Length: ${content.length + 1}`
                    const response = `HTTP/1.1 422 UNPROCESSABLE ENTITY\r\n${type}\r\n\r\n${content}`;
                    socket.write(response);
                    return
                }

                if (split_url[1] < 1 || split_url[1] > 5) {
                    const content = `Cliente não existe`;
                    const type = `Content-Type: text/plain\r\nContent-Length: ${content.length + 1}`
                    const response = `HTTP/1.1 404 NOT FOUND\r\n${type}\r\n\r\n${content}`;
                    socket.write(response);
                    return
                }
                handlePostClientes(socket, split_url[1], body);
                return
            }
        }
    }

    const content = `Endpoint não encontrado!`
    const type = `Content-Type: text/plain\r\nContent-Length: ${content.length + 1}`
    const response = `HTTP/1.1 404 NOT FOUND\r\n${type}\r\n\r\n${content}`;
    socket.write(response);
    return
});

async function handlePostClientes(socket, id, body) {
    //fazemos o parse do body
    let json_body;
    let error = false;
    var Message = `UNPROCESSABLE ENTITY`;

    try {
        json_body = JSON.parse(body, (key, value) => {
            if (key == 'valor') {
                if (!value || isNaN(value) || !Number.isInteger(value)) {
                    error = true;
                    Message = `valor precisa ser um número inteiroo`;
                }
            }

            if (key == 'tipo') {
                if (!value || (value != 'c' && value != 'd')) {
                    error = true
                    Message = `tipo precisa ser c ou d`;
                }
            }

            if (key == 'descricao') {
                if (!value || !isNaN(value) || value.length > 10) {
                    error = true
                    Message = `description precisa ser uma string com no maximo 10 caracteres`;
                }
            }

            return value
        });
    } catch (e) {
        const content = 'Houve um erro ao processar o corpo enviado.';
        const type = `Content-Type: text/plain\r\nContent-Length: ${content.length}`
        const response = `HTTP/1.1 422 UNPROCESSABLE ENTITY\r\n${type}\r\n\r\n${content}`;
        socket.write(response);
        return;
    }

    //depois, desestroturamos e validamos
    const { valor, tipo, descricao } = json_body;

    if (error) {
        const content = Message;
        const type = `Content-Type: text/plain\r\nContent-Length: ${content.length}`
        const response = `HTTP/1.1 422 UNPROCESSABLE ENTITY\r\n${type}\r\n\r\n${content}`;
        socket.write(response);
        return;
    }

    const cache = await CacheLib.execute({
        type: `transaction`,
        key: `transactions:${id}`,
        value: {
            valor,
            descricao,
            tipo
        }
    })

    if (cache.error) {
        if (cache.error.type == 'd') {
            const content = 'Saldo não cobre essa transação.';
            const type = `Content-Type: text/plain\r\nContent-Length: ${content.length}`
            const response = `HTTP/1.1 422 UNPROCESSABLE ENTITY\r\n${type}\r\n\r\n${content}`;
            socket.write(response);
            return;
        }
    }

    process.send(JSON.stringify({
        type: `db`,
        data: {
            id,
            valor,
            tipo,
            descricao
        }
    }))

    const content = JSON.stringify({
        saldo: cache.saldo.total,
        limite: cache.saldo.limite
    });
    const type = `Content-Type: application/json\r\nContent-Length: ${content.length}`
    const response = `HTTP/1.1 200 OK\r\n${type}\r\n\r\n${content}`;
    socket.write(response);
    return;
}

async function handleGetClientes(socket, id) {
    const cache = await CacheLib.execute({
        type: `get`,
        key: `transactions:${id}`
    })
    if (cache) {
        cache.saldo.data_extrato = new Date().toISOString();
        const content = JSON.stringify(cache)
        const type = `Content-Type: application/json\r\nContent-Length: ${content.length}`
        const response = `HTTP/1.1 200 OK\r\n${type}\r\n\r\n${content}`;

        socket.write(response);
        return
    }

    const extrato = await pool.query('SELECT obter_extrato($1)', [id]);
    const dt = extrato.rows[0].obter_extrato

    await CacheLib.execute({
        type: `put`,
        key: `transactions:${id}`,
        value: dt
    })

    const content = JSON.stringify(dt)
    const type = `Content-Type: application/json\r\nContent-Length: ${content.length + 1}`
    const response = `HTTP/1.1 200 OK\r\n${type}\r\n\r\n${content}`;
    socket.write(response);
    return
}

async function handlePostAdminDbReset(socket) {
    await CacheLib.clear(`clear`);
    await pool.query(`DELETE FROM transacoes`);
    await pool.query(`UPDATE clientes SET saldo = 0`);

    await RunCacheSeed();

    const content = JSON.stringify({ message: 'Banco de dados resetado com sucesso' })
    const type = `Content-Type: application/json\r\nContent-Length: ${content.length}`
    const response = `HTTP/1.1 200 OK\r\n${type}\r\n\r\n${content}`;
    socket.write(response);
    return
}

async function RunCacheSeed() {
    for (let i = 1; i <= 5; i++) {
        const data = await pool.query('SELECT obter_extrato($1)', [i])
        const response = data.rows[0].obter_extrato;
        await CacheLib.execute({
            type: `put`,
            key: `transactions:${i}`,
            value: response
        })
    }
}

async function tryConnect() {
    await new Promise((r, t) => setTimeout(() => r(true), 2000));
    try {
        await pool.connect();
        return;
    } catch (E) {
        return await tryConnect();
    }
}

async function run() {
    await tryConnect();
    cacheConnect = new ws(process.env.CACHE_URL)
    CacheLib = new Cache(); 
    await RunCacheSeed();
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, '0.0.0.0', process.env.BACKLOG || 512 * 8)
    console.log(`Servidor rodando na porta ${PORT}`);
}

async function setup() {
    await tryConnect();
}

if (cluster.isMaster) {
    setup();
    console.log(`Master ${process.pid} is running`);

    var queue = [];
    var isRunning = false;

    for (let i = 0; i < (process.env.INSTANCES || 1); i++) {
        cluster.fork();
    }

    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died`);
    });


    cluster.on('message', (worker, message, handle) => {
        const msg = JSON.parse(message);
        if(msg.type == `db`) {
            const { id, valor, tipo, descricao } = msg.data;

            queue.push({ id, valor, tipo, descricao })
            if (!isRunning) runQueue();
        }
    });

    const runQueue = async () => {
        if (isRunning || queue.length === 0) {
            return;
        }
        isRunning = true;

        const batchSize = 200;
        const batchInsertValues = [];

        while (queue.length > 0) {
            const { id, valor, tipo, descricao } = queue.shift();
            batchInsertValues.push({ id, valor, tipo, descricao });

            if (batchInsertValues.length === batchSize || queue.length === 0) {
                await insertBatch(batchInsertValues);
                batchInsertValues.length = 0;
            }
        }

        isRunning = false;
    }

    const insertBatch = async (values) => {
        try {
            var queryText = ``;
            for (let i = 0; i < values.length; i++) {
                queryText = queryText + `SELECT transacao(${values[i].id}, ${values[i].valor}, '${values[i].tipo}', '${values[i].descricao}'); `
            }

            await pool.query(queryText);
        } catch (error) {
            console.error('Erro ao inserir lote:', error);
        }
    }

} else {
    console.log(`Worker ${process.pid} started`);
    run();
}