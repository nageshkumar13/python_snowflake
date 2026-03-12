INSERT INTO RAW.SUPPORT_TICKETS_RAW (ticket_id, created_at, source, text_body)
SELECT *
FROM VALUES
    ('T1', CURRENT_TIMESTAMP(), 'web', 'Payment failed but money deducted from my account.'),
    ('T2', CURRENT_TIMESTAMP(), 'mobile', 'App crashes whenever I try to upload a file.'),
    ('T3', CURRENT_TIMESTAMP(), 'web', 'Need invoice copy for last month billing.'),
    ('T4', CURRENT_TIMESTAMP(), 'api', 'Login taking too long and sometimes timeout.'),
    ('T5', CURRENT_TIMESTAMP(), 'mobile', 'Please add dark mode feature.')
AS v(ticket_id, created_at, source, text_body)
WHERE NOT EXISTS (
    SELECT 1 FROM RAW.SUPPORT_TICKETS_RAW r
    WHERE r.ticket_id = v.ticket_id
);
