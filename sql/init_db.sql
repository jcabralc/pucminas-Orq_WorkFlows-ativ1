-- Criação do banco se não existir (tratado pelo entrypoint ou executado manualmente)
--CREATE DATABASE shopbrasil_db;

\c shopbrasil_db;

-- 1. Tabela de Snapshot (Mantém apenas o estado atualizado por categoria)
CREATE TABLE IF NOT EXISTS category_metrics_snapshot (
    category VARCHAR(255) PRIMARY KEY,
    quantity INT NOT NULL,
    avg_price NUMERIC(10, 2) NOT NULL,
    min_price NUMERIC(10, 2) NOT NULL,
    max_price NUMERIC(10, 2) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tabela de Histórico (Append-only diário, idempotente por execution_date)
CREATE TABLE IF NOT EXISTS category_metrics_history (
    category VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    avg_price NUMERIC(10, 2) NOT NULL,
    min_price NUMERIC(10, 2) NOT NULL,
    max_price NUMERIC(10, 2) NOT NULL,
    execution_date DATE NOT NULL,
    PRIMARY KEY (category, execution_date)
);