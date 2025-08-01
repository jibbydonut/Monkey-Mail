use database til_data_engineering;
use schema jacob_amp_python_s3;

select * from TIL_DATA_ENGINEERING.JACOB_AMP_PYTHON_S3.MAILCHIMP_REPORTS_EMAIL_ACTIVITY_PYTHON;
select * from TIL_DATA_ENGINEERING.JACOB_AMP_PYTHON_S3.MAILCHIMP_CAMPAIGN_LIST_PYTHON;


-- dim_campaign OLD
create or replace table monkey__dim_campaign as
SELECT
    campaign.value:id::string AS campaign_id,
    campaign.value:settings:subject_line::string AS subject_line,
    campaign.value:settings:from_name::string AS from_name,
    campaign.value:settings:reply_to::string AS reply_to,
    campaign.value:settings:title::string AS title,
    campaign.value:send_time::TIMESTAMP AS send_time,
    campaign.value:content_type::string as content_type,
    campaign.value:status::string as status,
    campaign.value:recipients.list_id::string as recipient_list_id

from mailchimp_campaign_list_python,
    LATERAL FLATTEN(input => parse_json(raw):campaigns) as campaign;

    
-- fact_campaign
create or replace table monkey__fact_campaign as
SELECT
    campaign.value:id::string AS campaign_id,
    campaign.value:emails_sent AS emails_sent,
    campaign.value:report_summary:open_rate AS open_rate,
    campaign.value:report_summary:click_rate AS click_rate,
    campaign.value:report_summary:opens AS opens,
    campaign.value:report_summary:clicks AS clicks,
    campaign.value:report_summary:unique_opens AS unique_opens
from mailchimp_campaign_list_python,
    LATERAL FLATTEN(input => parse_json(raw):campaigns) as campaign;

    
-- recipient_list - redundant??needed??
create or replace table monkey__recipient_list as
SELECT distinct
    campaign.value:recipients.list_id::string as recipient_list_id,
    campaign.value:recipients.list_is_active::boolean as list_is_active,
    campaign.value:recipients.list_name::string as list_name
from mailchimp_campaign_list_python,
    LATERAL FLATTEN(input => parse_json(raw):campaigns) as campaign;


-- emails
create or replace table monkey__emails as
select
    emails.value:email_id::string as email_id,
    emails.value:email_address::string as email_address,
    emails.value:campaign_id::string as campaign_id
from mailchimp_reports_email_activity_python,
    lateral flatten(input => parse_json(raw):emails) as emails;


-- activity
create or replace table monkey__campaign_activity as
with emails_flat as(
    select
        emails.value:email_id::string as email_id,
        emails.value:campaign_id::string as campaign_id,
        emails.value:activity as activity_array
    from mailchimp_reports_email_activity_python,
        lateral flatten(input => parse_json(raw):emails) as emails
),
activity_flat as (
    select
        e.email_id,
        e.campaign_id,
        a.value:"action"::string as action,
        a.value:ip as ip,
        a.value:"timestamp"::TIMESTAMP as time_stamp
    from emails_flat e,
    lateral flatten(input => e.activity_array) as a
)
select hash(email_id,ip,time_stamp) as activity_id, * from activity_flat;


-- dim_campaign DONT USE
--create or replace table monkey__dim_campaign as
with emails_flat as(
    select
        emails.value:email_id::string as email_id,
        emails.value:campaign_id::string as campaign_id,
        emails.value:activity as activity_array
    from mailchimp_reports_email_activity_python,
        lateral flatten(input => parse_json(raw):emails) as emails
),
activity_flat as (
    select
        e.email_id,
        e.campaign_id,
        a.value:"action"::string as action,
        a.value:ip as ip,
        a.value:"timestamp"::TIMESTAMP as time_stamp
    from emails_flat e,
    lateral flatten(input => e.activity_array) as a
),
dim_campaign as (
SELECT
    campaign.value:id::string AS campaign_id,
    campaign.value:settings:subject_line::string AS subject_line,
    campaign.value:settings:from_name::string AS from_name,
    campaign.value:settings:reply_to::string AS reply_to,
    campaign.value:settings:title::string AS title,
    campaign.value:send_time::TIMESTAMP AS send_time,
    campaign.value:content_type::string as content_type,
    campaign.value:status::string as status,
    campaign.value:recipients.list_id::string as recipient_list_id

from mailchimp_campaign_list_python,
    LATERAL FLATTEN(input => parse_json(raw):campaigns) as campaign
),
activity_join as (
select hash(email_id,ip,time_stamp) as activity_id, campaign_id
from activity_flat
)
select
    c.*,
    a.activity_id
from dim_campaign c
left join activity_join a
    on a.campaign_id = c.campaign_id;


-- dim_campaign procedure
create or replace procedure merge_monkey_dim_campaign()
returns string
language sql
as
$$
begin

    -- Merge new campaign data into target table
    merge into monkey__dim_campaign as target
    using (
        select
            campaign.value:id::string as campaign_id,
            campaign.value:settings:subject_line::string as subject_line,
            campaign.value:settings:from_name::string as from_name,
            campaign.value:settings:reply_to::string as reply_to,
            campaign.value:settings:title::string as title,
            campaign.value:send_time::timestamp as send_time,
            campaign.value:content_type::string as content_type,
            campaign.value:status::string as status,
            campaign.value:recipients.list_id::string as recipient_list_id
        from mailchimp_campaign_list_python,
            lateral flatten(input => parse_json(raw):campaigns) as campaign
    ) as source
    on target.campaign_id = source.campaign_id
    when matched then
        update set
            subject_line = source.subject_line,
            from_name = source.from_name,
            reply_to = source.reply_to,
            title = source.title,
            send_time = source.send_time,
            content_type = source.content_type,
            status = source.status,
            recipient_list_id = source.recipient_list_id
    when not matched then
        insert (
            campaign_id, subject_line, from_name, reply_to, title,
            send_time, content_type, status, recipient_list_id
        )
        values (
            source.campaign_id, source.subject_line, source.from_name, source.reply_to, source.title,
            source.send_time, source.content_type, source.status, source.recipient_list_id
        );

    return 'Merge completed successfully.';

end;
$$;

-- dim_campaign task
create or replace task task_refresh_monkey_dim_campaign
warehouse = dataschool_wh
schedule = '24 hours'
as
call merge_monkey_dim_campaign();


-- fact_campaign procedure
create or replace procedure merge_monkey_fact_campaign()
returns string
language sql
as
$$
BEGIN
    -- Merge new campaign facts data into target table
    merge into monkey__fact_campaign as target
    using (
        SELECT
            campaign.value:id::string AS campaign_id,
            campaign.value:emails_sent AS emails_sent,
            campaign.value:report_summary:open_rate AS open_rate,
            campaign.value:report_summary:click_rate AS click_rate,
            campaign.value:report_summary:opens AS opens,
            campaign.value:report_summary:clicks AS clicks,
            campaign.value:report_summary:unique_opens AS unique_opens
        from mailchimp_campaign_list_python,
            LATERAL FLATTEN(input => parse_json(raw):campaigns) as campaign
    ) as source
    on target.campaign_id = source.campaign_id
    when matched then
        update set
            emails_sent = source.emails_sent,
            open_rate = source.open_rate,
            click_rate = source.click_rate,
            opens = source.opens,
            clicks = source.clicks,
            unique_opens = source.unique_opens
    when not matched then
        insert (
            campaign_id, emails_sent, open_rate, click_rate, opens, clicks, unique_opens
        )
        values (
            source.campaign_id, source.emails_sent, source.open_rate, source.click_rate, source.opens,
            source.clicks, source.unique_opens
        );

    return 'Merge completed successfully.';

end;
$$;

-- fact_campaign task
create or replace task task_refresh_monkey_fact_campaign
warehouse = dataschool_wh
schedule = '24 hours'
as
call merge_monkey_fact_campaign();

