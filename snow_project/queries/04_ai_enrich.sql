MERGE INTO AI.SUPPORT_TICKETS_ENRICHED target
USING (
    SELECT
        r.ticket_id,
        r.created_at,
        r.text_body,

        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            CONCAT(
                'You are a classification AI. ',
                'Extract JSON with keys: category, urgency, sentiment, short_summary. ',
                'Return only JSON. Text: ',
                r.text_body
            )
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
    model
)
VALUES (
    src.ticket_id,
    src.created_at,
    src.text_body,
    src.ai_response,
    src.ai_response:"category"::STRING,
    src.ai_response:"urgency"::STRING,
    src.ai_response:"sentiment"::STRING,
    src.ai_response:"short_summary"::STRING,
    'mistral-large'
);
