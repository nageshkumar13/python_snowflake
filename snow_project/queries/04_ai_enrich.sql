MERGE INTO AI.SUPPORT_TICKETS_ENRICHED target
USING (
    SELECT
        r.ticket_id,
        r.created_at,
        r.text_body,
        SNOWFLAKE.CORTEX.COUNT_TOKENS(
        %(model)s,
        REPLACE(%(prompt)s, '{{TEXT}}', r.text_body)
       ) AS token_count,

        SNOWFLAKE.CORTEX.COMPLETE(
            %(model)s,
            REPLACE(%(prompt)s, '{{TEXT}}', r.text_body)
        ) AS ai_raw,

        TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE(
                %(model)s,
                REPLACE(%(prompt)s, '{{TEXT}}', r.text_body)
            )
        ) AS ai_json

    FROM RAW.SUPPORT_TICKETS_RAW r
) src
ON target.ticket_id = src.ticket_id

WHEN MATCHED THEN UPDATE SET
    ai_json        = src.ai_json,
    category       = src.ai_json:"category"::STRING,
    urgency        = src.ai_json:"urgency"::STRING,
    sentiment      = src.ai_json:"sentiment"::STRING,
    short_summary  = src.ai_json:"short_summary"::STRING,
    token_count    = src.token_count,
    model          = %(model)s,
    prompt_version = %(prompt_version)s,
    enriched_at    = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    ticket_id,
    created_at,
    text_body,
    ai_json,
    category,
    urgency,
    sentiment,
    short_summary,
    model,
    prompt_version,
    enriched_at,
    token_count
)
VALUES (
    src.ticket_id,
    src.created_at,
    src.text_body,
    src.ai_json,
    src.ai_json:"category"::STRING,
    src.ai_json:"urgency"::STRING,
    src.ai_json:"sentiment"::STRING,
    src.ai_json:"short_summary"::STRING,
    %(model)s,
    %(prompt_version)s,
    CURRENT_TIMESTAMP(),
    src.token_count
);
