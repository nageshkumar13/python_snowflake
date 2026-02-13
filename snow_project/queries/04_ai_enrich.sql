MERGE INTO AI.SUPPORT_TICKETS_ENRICHED target
USING (
    SELECT
        r.ticket_id,
        r.created_at,
        r.text_body,

        SNOWFLAKE.CORTEX.COMPLETE(
            %s,
            %s
        ) AS ai_response

    FROM RAW.SUPPORT_TICKETS_RAW r
) src
ON target.ticket_id = src.ticket_id

WHEN NOT MATCHED THEN
INSERT (
    ticket_id,
    created_at,
    text_body,
    ai_json,
    category,
    urgency,
    sentiment,
    short_summary,
    model,
    prompt_version
)
VALUES (
    src.ticket_id,
    src.created_at,
    src.text_body,
    PARSE_JSON(src.ai_response),

    PARSE_JSON(src.ai_response):"category"::STRING,
    PARSE_JSON(src.ai_response):"urgency"::STRING,
    PARSE_JSON(src.ai_response):"sentiment"::STRING,
    PARSE_JSON(src.ai_response):"short_summary"::STRING,
    %s,
    %s
);
