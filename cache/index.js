const dotenv = require('dotenv');
const WS = require("ws");

dotenv.config();

const PORT = process.env.PORT || 6456;

const server = new WS.Server({
    port: PORT,
}, () => console.log(`Servidor WebSocket está ouvindo em *:${PORT}`));

let cache = {};

const clearCache = () => {
    cache = {};
};

const addToQueue = (data) => {
    return new Promise((resolve, reject) => {
        const handleGet = () => {
            resolve({ value: cache[data.key] });
        };

        const handlePut = () => {
            cache[data.key] = data.value;
            resolve({ value: cache[data.key] });
        };

        const handleTransaction = () => {
            let dt = cache[data.key];
            const { saldo, ultimas_transacoes } = dt;
            const { valor, tipo, descricao } = data.value;

            if (tipo === 'd') {
                if (saldo.limite < ((saldo.total - valor) * -1)) {
                    return reject({ value: { error: { type: tipo, error: `no-limit` } } });
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

            cache[data.key] = {
                saldo,
                ultimas_transacoes: latest_transactions,
            };

            resolve({
                value: {
                    saldo,
                    ultimas_transacoes: latest_transactions,
                }
            });
        };

        switch (data.type) {
            case 'get':
                handleGet();
                break;
            case 'put':
                handlePut();
                break;
            case 'transaction':
                handleTransaction();
                break;
            default:
                reject({ error: 'Invalid operation type' });
                break;
        }
    });
};

server.on('connection', (socket) => {
    socket.on('message', async function(msg) {
        const parsed = JSON.parse(msg.toString(`utf-8`));
        if(parsed.queue == `clear`) clearCache();
        if(parsed.queue == `addToQueue`) {
            const data = parsed.data;
            const id = parsed.id;

            try {
                const result = await addToQueue(data);
                result.id = id;
                socket.send(JSON.stringify(result))
            } catch (error) {
                error.id = id;
                socket.send(JSON.stringify(error))
            }
        };
    });
});
