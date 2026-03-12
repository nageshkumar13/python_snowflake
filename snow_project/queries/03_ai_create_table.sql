CREATE OR REPLACE TABLE AI.SUPPORT_TICKETS_ENRICHED (
    ticket_id STRING,
    created_at TIMESTAMP,
    text_body STRING,

    ai_json VARIANT,

    category STRING,
    urgency STRING,
    sentiment STRING,
    short_summary STRING,
    token_count INTEGER,
    model STRING,
    prompt_version STRING,
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (ticket_id)
);