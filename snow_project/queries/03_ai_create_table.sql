CREATE TABLE IF NOT EXISTS AI.SUPPORT_TICKETS_ENRICHED (
    ticket_id STRING,
    created_at TIMESTAMP,
    text_body STRING,

    ai_json VARIANT,

    category STRING,
    urgency STRING,
    sentiment STRING,
    short_summary STRING,

    model STRING,
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (ticket_id)
);
