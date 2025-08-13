-- Blocks table - Partitioned by network
-- Token registry table (append-only CSV can be periodically merged into this)
CREATE TABLE IF NOT EXISTS tokens (
    token_contract TEXT PRIMARY KEY,
    symbol TEXT,
    decimals INTEGER,
    first_seen_block BIGINT,
    first_seen_at BIGINT
);

-- Historical prices table (one price per token per date)
CREATE TABLE IF NOT EXISTS prices (
    token_contract TEXT NOT NULL,
    date TEXT NOT NULL, -- format dd-mm-YYYY to align with CoinGecko history endpoint
    usd NUMERIC,
    PRIMARY KEY (token_contract, date)
);

CREATE TABLE IF NOT EXISTS blocks (
    chain VARCHAR(20) NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(128) NOT NULL,
    parent_hash VARCHAR(128),
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (chain, block_hash)
) PARTITION BY LIST (chain);

-- Create partitions for each network
CREATE TABLE IF NOT EXISTS blocks_ethereum PARTITION OF blocks FOR VALUES IN ('ethereum');
CREATE TABLE IF NOT EXISTS blocks_bitcoin PARTITION OF blocks FOR VALUES IN ('bitcoin');
CREATE TABLE IF NOT EXISTS blocks_xrp PARTITION OF blocks FOR VALUES IN ('xrp');
CREATE TABLE IF NOT EXISTS blocks_solana PARTITION OF blocks FOR VALUES IN ('solana');
CREATE TABLE IF NOT EXISTS blocks_bnb PARTITION OF blocks FOR VALUES IN ('bnb');
CREATE TABLE IF NOT EXISTS blocks_base PARTITION OF blocks FOR VALUES IN ('base');
CREATE TABLE IF NOT EXISTS blocks_arbitrum PARTITION OF blocks FOR VALUES IN ('arbitrum');
CREATE TABLE IF NOT EXISTS blocks_avalanche PARTITION OF blocks FOR VALUES IN ('avalanche');
CREATE TABLE IF NOT EXISTS blocks_polygon PARTITION OF blocks FOR VALUES IN ('polygon');
CREATE TABLE IF NOT EXISTS blocks_optimism PARTITION OF blocks FOR VALUES IN ('optimism');
CREATE TABLE IF NOT EXISTS blocks_polygonzk PARTITION OF blocks FOR VALUES IN ('polygonzk');
CREATE TABLE IF NOT EXISTS blocks_zksync PARTITION OF blocks FOR VALUES IN ('zksync');
CREATE TABLE IF NOT EXISTS blocks_mantle PARTITION OF blocks FOR VALUES IN ('mantle');
CREATE TABLE IF NOT EXISTS blocks_linea PARTITION OF blocks FOR VALUES IN ('linea');
CREATE TABLE IF NOT EXISTS blocks_tron PARTITION OF blocks FOR VALUES IN ('tron');

-- Block-specific indexes
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp 


    ON blocks USING brin (timestamp) WITH (pages_per_range = 128);
CREATE INDEX IF NOT EXISTS idx_blocks_hash 
    ON blocks USING btree (block_hash, chain);

-- EVM Transactions - Partitioned by network
CREATE TABLE IF NOT EXISTS evm_transactions (
    block_number BIGINT NOT NULL,
    chain VARCHAR(20) NOT NULL,
    transaction_hash VARCHAR(128) NOT NULL,
    chain_id BIGINT NOT NULL,
    from_address VARCHAR(64) NOT NULL,
    to_address VARCHAR(64),
    amount NUMERIC(78, 0) NOT NULL,
    total_gas BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (chain, timestamp, transaction_hash)
) PARTITION BY LIST (chain);

-- Create network partitions
CREATE TABLE IF NOT EXISTS evm_transactions_ethereum PARTITION OF evm_transactions FOR VALUES IN ('ethereum');
CREATE TABLE IF NOT EXISTS evm_transactions_bnb PARTITION OF evm_transactions FOR VALUES IN ('bnb');
CREATE TABLE IF NOT EXISTS evm_transactions_base PARTITION OF evm_transactions FOR VALUES IN ('base');
CREATE TABLE IF NOT EXISTS evm_transactions_arbitrum PARTITION OF evm_transactions FOR VALUES IN ('arbitrum');
CREATE TABLE IF NOT EXISTS evm_transactions_avalanche PARTITION OF evm_transactions FOR VALUES IN ('avalanche');
CREATE TABLE IF NOT EXISTS evm_transactions_polygon PARTITION OF evm_transactions FOR VALUES IN ('polygon');
CREATE TABLE IF NOT EXISTS evm_transactions_optimism PARTITION OF evm_transactions FOR VALUES IN ('optimism');
CREATE TABLE IF NOT EXISTS evm_transactions_polygonzk PARTITION OF evm_transactions FOR VALUES IN ('polygonzk');
CREATE TABLE IF NOT EXISTS evm_transactions_zksync PARTITION OF evm_transactions FOR VALUES IN ('zksync');
CREATE TABLE IF NOT EXISTS evm_transactions_mantle PARTITION OF evm_transactions FOR VALUES IN ('mantle');
CREATE TABLE IF NOT EXISTS evm_transactions_linea PARTITION OF evm_transactions FOR VALUES IN ('linea');
CREATE TABLE IF NOT EXISTS evm_transactions_tron PARTITION OF evm_transactions FOR VALUES IN ('tron');



-- EVM transaction indexes
CREATE INDEX IF NOT EXISTS idx_evm_tx_timestamp 
    ON evm_transactions USING brin (timestamp) WITH (pages_per_range = 128);
CREATE INDEX IF NOT EXISTS idx_evm_tx_from 
    ON evm_transactions USING btree (from_address, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_evm_tx_to 
    ON evm_transactions USING btree (to_address, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_evm_tx_hash 
    ON evm_transactions USING btree (transaction_hash);
CREATE INDEX IF NOT EXISTS idx_evm_tx_chain 
    ON evm_transactions USING btree (chain, timestamp DESC);


-- Bitcoin Transactions
CREATE TABLE IF NOT EXISTS bitcoin_transactions (
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(128) NOT NULL,
    version SMALLINT NOT NULL,
    amount NUMERIC(20, 0) NOT NULL,
    timestamp BIGINT NOT NULL,
    fee NUMERIC(16, 8) NOT NULL,
    CONSTRAINT pk_bitcoin_transactions PRIMARY KEY (timestamp, transaction_hash)
);

-- Bitcoin transaction indexes
CREATE INDEX IF NOT EXISTS idx_bitcoin_tx_timestamp 
    ON bitcoin_transactions USING brin (timestamp) WITH (pages_per_range = 128);
CREATE INDEX IF NOT EXISTS idx_bitcoin_tx_hash 
    ON bitcoin_transactions USING btree (transaction_hash);
CREATE INDEX IF NOT EXISTS idx_bitcoin_tx_block 
    ON bitcoin_transactions USING btree (block_number);


-- XRP Transactions
CREATE TABLE IF NOT EXISTS xrp_transactions (
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(128) NOT NULL,
    account VARCHAR(64) NOT NULL,
    type VARCHAR(20) NOT NULL,
    fee BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (timestamp, transaction_hash)
);

-- XRP transaction indexes
CREATE INDEX IF NOT EXISTS idx_xrp_tx_timestamp 
    ON xrp_transactions USING brin (timestamp) WITH (pages_per_range = 128);
CREATE INDEX IF NOT EXISTS idx_xrp_tx_hash 
    ON xrp_transactions USING btree (transaction_hash);
CREATE INDEX IF NOT EXISTS idx_xrp_tx_account 
    ON xrp_transactions USING btree (account, timestamp DESC);

DROP TABLE IF EXISTS base_solana_transactions;

-- Solana Transactions
CREATE TABLE IF NOT EXISTS solana_transactions (
    block_number BIGINT NOT NULL,
    signature VARCHAR(128) NOT NULL,
    amount BIGINT NOT NULL,
    fee BIGINT NOT NULL,
    account_key VARCHAR(64) NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (timestamp, signature)
);
CREATE INDEX IF NOT EXISTS idx_solana_tx_timestamp 
    ON solana_transactions USING brin (timestamp) WITH (pages_per_range = 128);
-- Is this needed?
CREATE INDEX IF NOT EXISTS idx_solana_tx_signature 
    ON solana_transactions USING btree (signature);

CREATE INDEX IF NOT EXISTS idx_solana_tx_account 
    ON solana_transactions USING btree (account_key, timestamp DESC);

-- EVM Decoded Events -- Holds the decoded events, based on the signature hash for each chain, helps to determine non-native actions
CREATE TABLE IF NOT EXISTS evm_decoded_events (
    chain VARCHAR(20) NOT NULL,
    signature_hash VARCHAR(128) NOT NULL,
    event_name TEXT NOT NULL,
    decoded_signature TEXT NOT NULL,
    input_types TEXT NOT NULL,
    indexed_inputs TEXT NOT NULL,
    input_names TEXT NOT NULL,
    inputs TEXT NOT NULL,
    PRIMARY KEY (chain, signature_hash)
) PARTITION BY LIST (chain);

-- EVM Partitions
CREATE TABLE IF NOT EXISTS evm_decoded_events_ethereum PARTITION OF evm_decoded_events FOR VALUES IN ('ethereum');
CREATE TABLE IF NOT EXISTS evm_decoded_events_bnb PARTITION OF evm_decoded_events FOR VALUES IN ('bnb');
CREATE TABLE IF NOT EXISTS evm_decoded_events_base PARTITION OF evm_decoded_events FOR VALUES IN ('base');
CREATE TABLE IF NOT EXISTS evm_decoded_events_arbitrum PARTITION OF evm_decoded_events FOR VALUES IN ('arbitrum');
CREATE TABLE IF NOT EXISTS evm_decoded_events_avalanche PARTITION OF evm_decoded_events FOR VALUES IN ('avalanche');
CREATE TABLE IF NOT EXISTS evm_decoded_events_polygon PARTITION OF evm_decoded_events FOR VALUES IN ('polygon');
CREATE TABLE IF NOT EXISTS evm_decoded_events_optimism PARTITION OF evm_decoded_events FOR VALUES IN ('optimism');
CREATE TABLE IF NOT EXISTS evm_decoded_events_polygonzk PARTITION OF evm_decoded_events FOR VALUES IN ('polygonzk');
CREATE TABLE IF NOT EXISTS evm_decoded_events_zksync PARTITION OF evm_decoded_events FOR VALUES IN ('zksync');
CREATE TABLE IF NOT EXISTS evm_decoded_events_mantle PARTITION OF evm_decoded_events FOR VALUES IN ('mantle');
CREATE TABLE IF NOT EXISTS evm_decoded_events_linea PARTITION OF evm_decoded_events FOR VALUES IN ('linea');
CREATE TABLE IF NOT EXISTS evm_decoded_events_tron PARTITION OF evm_decoded_events FOR VALUES IN ('tron');


CREATE INDEX IF NOT EXISTS idx_evm_events_signature 
    ON evm_decoded_events USING btree (signature_hash);


-- Create new partitioned contract ABIs table
CREATE TABLE IF NOT EXISTS evm_contract_abis (
    chain VARCHAR(20) NOT NULL,
    contract_address VARCHAR(64) NOT NULL,
    abi TEXT NOT NULL,
    CONSTRAINT pk_evm_contract_abis PRIMARY KEY (chain, contract_address)
) PARTITION BY LIST (chain);

-- EVM Partitions
CREATE TABLE IF NOT EXISTS evm_contract_abis_ethereum PARTITION OF evm_contract_abis FOR VALUES IN ('ethereum');
CREATE TABLE IF NOT EXISTS evm_contract_abis_bnb PARTITION OF evm_contract_abis FOR VALUES IN ('bnb');
CREATE TABLE IF NOT EXISTS evm_contract_abis_base PARTITION OF evm_contract_abis FOR VALUES IN ('base');
CREATE TABLE IF NOT EXISTS evm_contract_abis_arbitrum PARTITION OF evm_contract_abis FOR VALUES IN ('arbitrum');
CREATE TABLE IF NOT EXISTS evm_contract_abis_avalanche PARTITION OF evm_contract_abis FOR VALUES IN ('avalanche');
CREATE TABLE IF NOT EXISTS evm_contract_abis_polygon PARTITION OF evm_contract_abis FOR VALUES IN ('polygon');
CREATE TABLE IF NOT EXISTS evm_contract_abis_optimism PARTITION OF evm_contract_abis FOR VALUES IN ('optimism');
CREATE TABLE IF NOT EXISTS evm_contract_abis_polygonzk PARTITION OF evm_contract_abis FOR VALUES IN ('polygonzk');
CREATE TABLE IF NOT EXISTS evm_contract_abis_zksync PARTITION OF evm_contract_abis FOR VALUES IN ('zksync');
CREATE TABLE IF NOT EXISTS evm_contract_abis_mantle PARTITION OF evm_contract_abis FOR VALUES IN ('mantle');
CREATE TABLE IF NOT EXISTS evm_contract_abis_linea PARTITION OF evm_contract_abis FOR VALUES IN ('linea');
CREATE TABLE IF NOT EXISTS evm_contract_abis_tron PARTITION OF evm_contract_abis FOR VALUES IN ('tron');


-- EVM Contract ABIs Indexes
CREATE INDEX IF NOT EXISTS idx_evm_contract_abis_address 
    ON evm_contract_abis USING btree (contract_address, chain);

-- EVM Swap Info table

CREATE TABLE IF NOT EXISTS evm_swap_info (
    contract_address VARCHAR(64) NOT NULL,
    factory_address VARCHAR(64) NOT NULL,
    fee INT,
    token0_name VARCHAR(100),
    token1_name VARCHAR(100),
    token0_symbol VARCHAR(100),
    token1_symbol VARCHAR(100),
    token0_decimals SMALLINT,
    token1_decimals SMALLINT,
    token0_address VARCHAR(64),
    token1_address VARCHAR(64),
    name VARCHAR(100),
    chain VARCHAR(20) NOT NULL,
    PRIMARY KEY (contract_address, chain)
) PARTITION BY LIST (chain);

CREATE TABLE IF NOT EXISTS evm_swap_info_ethereum PARTITION OF evm_swap_info FOR VALUES IN ('ethereum');
CREATE TABLE IF NOT EXISTS evm_swap_info_bnb PARTITION OF evm_swap_info FOR VALUES IN ('bnb');
CREATE TABLE IF NOT EXISTS evm_swap_info_base PARTITION OF evm_swap_info FOR VALUES IN ('base');
CREATE TABLE IF NOT EXISTS evm_swap_info_arbitrum PARTITION OF evm_swap_info FOR VALUES IN ('arbitrum');
CREATE TABLE IF NOT EXISTS evm_swap_info_avalanche PARTITION OF evm_swap_info FOR VALUES IN ('avalanche');
CREATE TABLE IF NOT EXISTS evm_swap_info_polygon PARTITION OF evm_swap_info FOR VALUES IN ('polygon');
CREATE TABLE IF NOT EXISTS evm_swap_info_optimism PARTITION OF evm_swap_info FOR VALUES IN ('optimism');
CREATE TABLE IF NOT EXISTS evm_swap_info_polygonzk PARTITION OF evm_swap_info FOR VALUES IN ('polygonzk');
CREATE TABLE IF NOT EXISTS evm_swap_info_zksync PARTITION OF evm_swap_info FOR VALUES IN ('zksync');
CREATE TABLE IF NOT EXISTS evm_swap_info_mantle PARTITION OF evm_swap_info FOR VALUES IN ('mantle');
CREATE TABLE IF NOT EXISTS evm_swap_info_linea PARTITION OF evm_swap_info FOR VALUES IN ('linea');
CREATE TABLE IF NOT EXISTS evm_swap_info_tron PARTITION OF evm_swap_info FOR VALUES IN ('tron');


-- For when we want to get all the swap info of a contract
CREATE INDEX IF NOT EXISTS idx_evm_swap_contract 
    ON evm_swap_info USING btree (contract_address, chain);

-- For when we want to get all the swaps for a factory
CREATE INDEX IF NOT EXISTS idx_evm_swap_factory 
    ON evm_swap_info USING btree (factory_address, chain);


-- EVM Token Info table

CREATE TABLE IF NOT EXISTS evm_token_info (
    contract_address VARCHAR(64) NOT NULL,
    name VARCHAR(100),
    symbol VARCHAR(100),
    decimals INT NOT NULL,
    chain VARCHAR(20) NOT NULL,
    CONSTRAINT pk_evm_token_info PRIMARY KEY (contract_address, chain)
) PARTITION BY LIST (chain);

CREATE TABLE IF NOT EXISTS evm_token_info_ethereum PARTITION OF evm_token_info FOR VALUES IN ('ethereum');
CREATE TABLE IF NOT EXISTS evm_token_info_bnb PARTITION OF evm_token_info FOR VALUES IN ('bnb');
CREATE TABLE IF NOT EXISTS evm_token_info_base PARTITION OF evm_token_info FOR VALUES IN ('base');
CREATE TABLE IF NOT EXISTS evm_token_info_arbitrum PARTITION OF evm_token_info FOR VALUES IN ('arbitrum');
CREATE TABLE IF NOT EXISTS evm_token_info_avalanche PARTITION OF evm_token_info FOR VALUES IN ('avalanche');
CREATE TABLE IF NOT EXISTS evm_token_info_polygon PARTITION OF evm_token_info FOR VALUES IN ('polygon');
CREATE TABLE IF NOT EXISTS evm_token_info_optimism PARTITION OF evm_token_info FOR VALUES IN ('optimism');
CREATE TABLE IF NOT EXISTS evm_token_info_polygonzk PARTITION OF evm_token_info FOR VALUES IN ('polygonzk');
CREATE TABLE IF NOT EXISTS evm_token_info_zksync PARTITION OF evm_token_info FOR VALUES IN ('zksync');
CREATE TABLE IF NOT EXISTS evm_token_info_mantle PARTITION OF evm_token_info FOR VALUES IN ('mantle');
CREATE TABLE IF NOT EXISTS evm_token_info_linea PARTITION OF evm_token_info FOR VALUES IN ('linea');
CREATE TABLE IF NOT EXISTS evm_token_info_tron PARTITION OF evm_token_info FOR VALUES IN ('tron');

CREATE INDEX IF NOT EXISTS idx_evm_token_info_address ON evm_token_info
    USING btree (contract_address, chain);

-- Change creator to factory
CREATE TABLE IF NOT EXISTS evm_contract_to_factory (
    contract_address VARCHAR(64) NOT NULL,
    factory_address VARCHAR(64) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    CONSTRAINT pk_evm_contract_to_factory PRIMARY KEY (contract_address, chain)
) PARTITION BY LIST (chain);

-- Factory Partitions
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_ethereum PARTITION OF evm_contract_to_factory FOR VALUES IN ('ethereum');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_bnb PARTITION OF evm_contract_to_factory FOR VALUES IN ('bnb');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_base PARTITION OF evm_contract_to_factory FOR VALUES IN ('base');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_arbitrum PARTITION OF evm_contract_to_factory FOR VALUES IN ('arbitrum');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_avalanche PARTITION OF evm_contract_to_factory FOR VALUES IN ('avalanche');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_polygon PARTITION OF evm_contract_to_factory FOR VALUES IN ('polygon');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_optimism PARTITION OF evm_contract_to_factory FOR VALUES IN ('optimism');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_polygonzk PARTITION OF evm_contract_to_factory FOR VALUES IN ('polygonzk');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_zksync PARTITION OF evm_contract_to_factory FOR VALUES IN ('zksync');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_mantle PARTITION OF evm_contract_to_factory FOR VALUES IN ('mantle');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_linea PARTITION OF evm_contract_to_factory FOR VALUES IN ('linea');
CREATE TABLE IF NOT EXISTS evm_contract_to_factory_tron PARTITION OF evm_contract_to_factory FOR VALUES IN ('tron');


-- Factory Indexes
CREATE INDEX IF NOT EXISTS idx_evm_contract_to_factory_contract 
    ON evm_contract_to_factory USING btree (contract_address, chain);

CREATE INDEX IF NOT EXISTS idx_evm_contract_to_factory_factory 
    ON evm_contract_to_factory USING btree (factory_address, chain);

-- Adjust so that it can hold all the info about a token, contract, name, decimals, symbol, etc.
CREATE TABLE IF NOT EXISTS evm_swaps (
    chain VARCHAR(20) NOT NULL,
    contract_address VARCHAR(64) NOT NULL,
    transaction_hash VARCHAR(128) NOT NULL,
    log_index INT NOT NULL,
    timestamp BIGINT NOT NULL,
    amount0 NUMERIC(78, 36) NOT NULL,
    amount1 NUMERIC(78, 36) NOT NULL,
    token0_address VARCHAR(64) NOT NULL,
    token1_address VARCHAR(64) NOT NULL,
    token0_name VARCHAR(100),
    token1_name VARCHAR(100),
    token0_symbol VARCHAR(100),
    token1_symbol VARCHAR(100),
    factory_address VARCHAR(64),
    name VARCHAR(100),
    PRIMARY KEY (chain, transaction_hash, log_index)
) PARTITION BY LIST (chain);


-- EVM Partitions
CREATE TABLE IF NOT EXISTS evm_swaps_ethereum PARTITION OF evm_swaps FOR VALUES IN ('ethereum');
CREATE TABLE IF NOT EXISTS evm_swaps_bnb PARTITION OF evm_swaps FOR VALUES IN ('bnb');
CREATE TABLE IF NOT EXISTS evm_swaps_base PARTITION OF evm_swaps FOR VALUES IN ('base');
CREATE TABLE IF NOT EXISTS evm_swaps_arbitrum PARTITION OF evm_swaps FOR VALUES IN ('arbitrum');
CREATE TABLE IF NOT EXISTS evm_swaps_avalanche PARTITION OF evm_swaps FOR VALUES IN ('avalanche');
CREATE TABLE IF NOT EXISTS evm_swaps_polygon PARTITION OF evm_swaps FOR VALUES IN ('polygon');
CREATE TABLE IF NOT EXISTS evm_swaps_optimism PARTITION OF evm_swaps FOR VALUES IN ('optimism');
CREATE TABLE IF NOT EXISTS evm_swaps_polygonzk PARTITION OF evm_swaps FOR VALUES IN ('polygonzk');
CREATE TABLE IF NOT EXISTS evm_swaps_zksync PARTITION OF evm_swaps FOR VALUES IN ('zksync');
CREATE TABLE IF NOT EXISTS evm_swaps_mantle PARTITION OF evm_swaps FOR VALUES IN ('mantle');
CREATE TABLE IF NOT EXISTS evm_swaps_linea PARTITION OF evm_swaps FOR VALUES IN ('linea');
CREATE TABLE IF NOT EXISTS evm_swaps_tron PARTITION OF evm_swaps FOR VALUES IN ('tron');


CREATE INDEX IF NOT EXISTS idx_evm_swaps_timestamp 
    ON evm_swaps USING brin (timestamp) WITH (pages_per_range = 128);
CREATE INDEX IF NOT EXISTS idx_evm_swaps_tx 
    ON evm_swaps USING btree (transaction_hash, chain);
CREATE INDEX IF NOT EXISTS idx_evm_swaps_contract 
    ON evm_swaps USING btree (contract_address, chain);
CREATE INDEX IF NOT EXISTS idx_evm_swaps_tokens 
    ON evm_swaps USING btree (token0_address, token1_address, chain);


CREATE TABLE IF NOT EXISTS evm_syncs (
    chain VARCHAR(20) NOT NULL,
    contract_address VARCHAR(64) NOT NULL,
    transaction_hash VARCHAR(128) NOT NULL,
    log_index INT NOT NULL,
    timestamp BIGINT NOT NULL,
    reserve0 NUMERIC(78, 36) NOT NULL,
    reserve1 NUMERIC(78, 36) NOT NULL,
    token0_address VARCHAR(64) NOT NULL,
    token1_address VARCHAR(64) NOT NULL,
    token0_name VARCHAR(100),
    token1_name VARCHAR(100),
    token0_symbol VARCHAR(100),
    token1_symbol VARCHAR(100),
    factory_address VARCHAR(64),
    name VARCHAR(100),
    PRIMARY KEY (chain, transaction_hash, log_index)
) PARTITION BY LIST (chain);


CREATE TABLE IF NOT EXISTS evm_syncs_ethereum PARTITION OF evm_syncs FOR VALUES IN ('ethereum');
CREATE TABLE IF NOT EXISTS evm_syncs_bnb PARTITION OF evm_syncs FOR VALUES IN ('bnb');
CREATE TABLE IF NOT EXISTS evm_syncs_base PARTITION OF evm_syncs FOR VALUES IN ('base');
CREATE TABLE IF NOT EXISTS evm_syncs_arbitrum PARTITION OF evm_syncs FOR VALUES IN ('arbitrum');
CREATE TABLE IF NOT EXISTS evm_syncs_avalanche PARTITION OF evm_syncs FOR VALUES IN ('avalanche');
CREATE TABLE IF NOT EXISTS evm_syncs_polygon PARTITION OF evm_syncs FOR VALUES IN ('polygon');
CREATE TABLE IF NOT EXISTS evm_syncs_optimism PARTITION OF evm_syncs FOR VALUES IN ('optimism');
CREATE TABLE IF NOT EXISTS evm_syncs_mantle PARTITION OF evm_syncs FOR VALUES IN ('mantle');
CREATE TABLE IF NOT EXISTS evm_syncs_linea PARTITION OF evm_syncs FOR VALUES IN ('linea');
CREATE TABLE IF NOT EXISTS evm_syncs_polygonzk PARTITION OF evm_syncs FOR VALUES IN ('polygonzk');
CREATE TABLE IF NOT EXISTS evm_syncs_zksync PARTITION OF evm_syncs FOR VALUES IN ('zksync');
CREATE TABLE IF NOT EXISTS evm_syncs_tron PARTITION OF evm_syncs FOR VALUES IN ('tron');


CREATE INDEX IF NOT EXISTS idx_evm_syncs_timestamp 
    ON evm_syncs USING brin (timestamp) WITH (pages_per_range = 128);
CREATE INDEX IF NOT EXISTS idx_evm_syncs_tx 
    ON evm_syncs USING btree (transaction_hash, chain);
CREATE INDEX IF NOT EXISTS idx_evm_syncs_contract 
    ON evm_syncs USING btree (contract_address, chain);


-- Trader Intelligence additions: token_transfers, dex_swaps, and pipeline_state

-- Holds ERC-20 Transfer events enriched by the Base pipeline
CREATE TABLE IF NOT EXISTS token_transfers (
    transaction_hash VARCHAR(128) NOT NULL,
    log_index INT NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    chain VARCHAR(20) NOT NULL DEFAULT 'base',
    token_contract VARCHAR(64) NOT NULL,
    token_symbol VARCHAR(100),
    from_address VARCHAR(64) NOT NULL,
    to_address VARCHAR(64) NOT NULL,
    value NUMERIC(78, 36),
    usd_value NUMERIC(38, 8),
    CONSTRAINT pk_token_transfers PRIMARY KEY (transaction_hash, log_index)
);

-- Helpful indexes for analytics
CREATE INDEX IF NOT EXISTS idx_token_transfers_timestamp
    ON token_transfers USING brin (timestamp) WITH (pages_per_range = 128);
CREATE INDEX IF NOT EXISTS idx_token_transfers_token
    ON token_transfers USING btree (token_contract, chain);
CREATE INDEX IF NOT EXISTS idx_token_transfers_from
    ON token_transfers USING btree (from_address, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_token_transfers_to
    ON token_transfers USING btree (to_address, timestamp DESC);


-- Placeholder for DEX swap events (future extension)
CREATE TABLE IF NOT EXISTS dex_swaps (
    transaction_hash VARCHAR(128) NOT NULL,
    log_index INT NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    chain VARCHAR(20) NOT NULL DEFAULT 'base',
    pool_contract VARCHAR(64) NOT NULL,
    token_in_contract VARCHAR(64) NOT NULL,
    token_in_symbol VARCHAR(100),
    amount_in NUMERIC(78, 36),
    usd_value_in NUMERIC(38, 8),
    token_out_contract VARCHAR(64) NOT NULL,
    token_out_symbol VARCHAR(100),
    amount_out NUMERIC(78, 36),
    usd_value_out NUMERIC(38, 8),
    CONSTRAINT pk_dex_swaps PRIMARY KEY (transaction_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_dex_swaps_timestamp
    ON dex_swaps USING brin (timestamp) WITH (pages_per_range = 128);
CREATE INDEX IF NOT EXISTS idx_dex_swaps_pool
    ON dex_swaps USING btree (pool_contract, chain);
CREATE INDEX IF NOT EXISTS idx_dex_swaps_tokens
    ON dex_swaps USING btree (token_in_contract, token_out_contract, chain);


-- Tracks last processed block for each chain for robust restarts
CREATE TABLE IF NOT EXISTS pipeline_state (
    chain VARCHAR(20) PRIMARY KEY,
    last_processed_block BIGINT NOT NULL
);
