MERGE INTO AI.SUPPORT_TICKETS_ENRICHED AS target
USING (

    WITH ai_calc AS (
        SELECT
            r.ticket_id,
            r.created_at,
            r.text_body,

            REPLACE(%(prompt)s, '{{TEXT}}', r.text_body) AS final_prompt

        FROM RAW.SUPPORT_TICKETS_RAW r
    ),

    ai_response AS (
        SELECT
            ticket_id,
            created_at,
            text_body,

            SNOWFLAKE.CORTEX.COUNT_TOKENS(
                %(model)s,
                final_prompt
            ) AS token_count,

            SNOWFLAKE.CORTEX.COMPLETE(
                %(model)s,
                final_prompt
            ) AS ai_raw

        FROM ai_calc
    )

    SELECT
        ticket_id,
        created_at,
        text_body,
        token_count,
        ai_raw,
        TRY_PARSE_JSON(ai_raw) AS ai_json
    FROM ai_response

) AS src

ON target.ticket_id = src.ticket_id

WHEN MATCHED THEN UPDATE SET
    target.ai_json        = src.ai_json,
    target.category       = src.ai_json:"category"::STRING,
    target.urgency        = src.ai_json:"urgency"::STRING,
    target.sentiment      = src.ai_json:"sentiment"::STRING,
    target.short_summary  = src.ai_json:"short_summary"::STRING,
    target.token_count    = src.token_count,
    target.model          = %(model)s,
    target.prompt_version = %(prompt_version)s,
    target.enriched_at    = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    ticket_id,
    created_at,
    text_body,
    ai_json,
    category,
    urgency,
    sentiment,
    short_summary,
    token_count,
    model,
    prompt_version,
    enriched_at
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
    src.token_count,
    %(model)s,
    %(prompt_version)s,
    CURRENT_TIMESTAMP()
);