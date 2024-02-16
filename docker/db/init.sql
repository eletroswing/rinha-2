CREATE TABLE clientes (
	id SERIAL PRIMARY KEY,
	nome VARCHAR(50) NOT NULL,
	limite INTEGER NOT NULL,
	saldo INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE transacoes (
	id SERIAL PRIMARY KEY,
	cliente_id INTEGER NOT NULL,
	valor INTEGER NOT NULL,
	tipo CHAR(1) NOT NULL,
	descricao VARCHAR(10) NOT NULL,
	realizada_em TIMESTAMP NOT NULL DEFAULT NOW(),
	CONSTRAINT fk_clientes_transacoes_id
		FOREIGN KEY (cliente_id) REFERENCES clientes(id)
);


-- INDEX
CREATE INDEX idx_clientes_id ON clientes (id);
CREATE INDEX idx_transacoes_cliente_id_data ON transacoes (cliente_id, realizada_em);
CREATE INDEX idx_transacoes_realizada_em ON transacoes (realizada_em);

-- FUNCTIONS
-- OBTER EXTRATO WITH RECENT TRANSACTIONS
CREATE OR REPLACE FUNCTION obter_extrato(cliente_id_param INTEGER)
RETURNS JSON AS $$
DECLARE
    extrato JSON;
BEGIN
    SELECT json_build_object(
        'saldo', json_build_object(
            'total', c.saldo,
            'data_extrato', NOW(),
            'limite', c.limite
        ),
        'ultimas_transacoes', COALESCE((
            SELECT json_agg(json_build_object(
                'valor', t.valor,
                'tipo', t.tipo,
                'descricao', t.descricao,
                'realizada_em', t.realizada_em
            ) ORDER BY t.realizada_em DESC)
            FROM transacoes t
            WHERE t.cliente_id = cliente_id_param
            LIMIT 10
        ), '[]'::JSON)
    )
    INTO extrato
    FROM clientes c
    WHERE c.id = cliente_id_param;

    RETURN extrato;
END;
$$ LANGUAGE plpgsql;

-- INSERT TRANSACTIONS
CREATE OR REPLACE FUNCTION transacao(cliente_id_param INTEGER, valor_param INTEGER, tipo_param CHAR(1), descricao_param VARCHAR(10))
RETURNS VOID AS $$
DECLARE
    saldo_atual INTEGER;
    limite_atual INTEGER;
    novo_saldo INTEGER;
BEGIN
    SELECT saldo, limite INTO saldo_atual, limite_atual FROM clientes WHERE id = cliente_id_param;
    INSERT INTO transacoes (cliente_id, valor, tipo, descricao)
          VALUES (cliente_id_param, valor_param, tipo_param, descricao_param);

    IF (tipo_param = 'c') THEN
        novo_saldo := saldo_atual + valor_param;

    ELSE
        novo_saldo := saldo_atual - valor_param;
    END IF;
    UPDATE clientes SET saldo = novo_saldo WHERE id = cliente_id_param;

    RETURN;
END;
$$ LANGUAGE plpgsql;

--- SEED
DO $$
BEGIN
	INSERT INTO clientes (nome, limite)
	VALUES
		('user 1', 1000 * 100),
		('user 2', 800 * 100),
		('user 3', 10000 * 100),
		('user 4', 100000 * 100),
		('user 5', 5000 * 100);
END;
$$;