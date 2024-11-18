# Cohort-Activity-Based---Daily

WITH
baseline_day_register AS (
   
    SELECT 
        DISTINCT
        date_day
    FROM dbt_prod.date_spine_day
    WHERE 1=1 
        AND date_day < current_date
        AND {{event_at}} 
),

is_trx_reguler AS (
    
    SELECT 
        DISTINCT user_id,
        is_reguler_physical
    FROM 
        dbt_prod.REX_PRODUCT_ORDER_DETAIL
    WHERE 
        is_reguler_physical = 1
        AND DATE(payment_at) IN (SELECT date_day FROM baseline_day_register)
),

survey_data AS (
  
    SELECT user_id,
           question_text,
           answer_text,
           question_text || ' - ' || answer_text AS question_and_answer
    FROM dbt.dbt_prod.dim_reseller_survey_answer
    WHERE 1=1
    [[AND question_and_answer = {{question_answer_survey}}]]

),

user_survey AS (
   
    SELECT DISTINCT user_id
    FROM survey_data
),

user_phase_detail AS (
   
    SELECT MART_RESELLER_ACQUISITION_PHASE.month_at,
          MART_RESELLER_ACQUISITION_PHASE.user_id,
          MART_RESELLER_ACQUISITION_PHASE.RESELLER_ACTIVE_PHASE_LEVEL_1,
          MART_RESELLER_ACQUISITION_PHASE.RESELLER_ACTIVE_PHASE_LEVEL_2,
          MART_RESELLER_ACQUISITION_PHASE.ACQUISITION_PHASE,
          is_trx_reguler.is_reguler_physical,
          CASE 
              WHEN user_survey.user_id IS NOT NULL THEN 1
              ELSE 0
          END AS is_survey_user
    FROM dbt_prod.MART_RESELLER_ACQUISITION_PHASE
    JOIN dbt_prod.GROSS_RESELLER ON MART_RESELLER_ACQUISITION_PHASE.user_id = GROSS_RESELLER.user_id
    LEFT JOIN is_trx_reguler ON MART_RESELLER_ACQUISITION_PHASE.user_id = is_trx_reguler.user_id
    LEFT JOIN user_survey ON MART_RESELLER_ACQUISITION_PHASE.user_id = user_survey.user_id
    WHERE MART_RESELLER_ACQUISITION_PHASE.month_at IN (SELECT DISTINCT DATE_TRUNC('month', date_day) FROM baseline_day_register)
    [[AND {{RESELLER_PHASE_LEVEL_1}}]]
    [[AND {{RESELLER_PHASE_LEVEL_2}}]]
    [[AND {{PHASE_DETAIL}}]]
    [[AND {{RESELLER_PROJECT}}]]
    [[AND 
        CASE
            WHEN {{is_trx_reguler}} = '1' THEN is_trx_reguler.is_reguler_physical = 1
            WHEN {{is_trx_reguler}} = '0' THEN is_trx_reguler.is_reguler_physical is null
            WHEN {{is_trx_reguler}} = 'All' THEN 1=1
        END
    ]]
    [[AND 
        CASE
            WHEN {{question_answer_survey}} IS NOT NULL then is_survey_user = 1
            WHEN {{question_answer_survey}} is null then 1=1
        END
    ]]   
),

centralized_monitoring AS (
   
    SELECT period,
           user_id
    FROM dbt_prod.TF_CENTRALIZED_MONITORING_USER
    WHERE 1=1
    AND {{phase}}
    [[AND CASE WHEN {{is_exclude_krs}} = 'Yes' THEN COALESCE(is_krs, 0) = 0 END]]
    [[AND CASE WHEN {{is_exclude_special_initiative}} = 'Yes' THEN COALESCE(is_special_initiative, 0) = 0 END]]
),    
    
    
dau_d00 AS (
    
    SELECT dau.period AS start_date,
           up.month_at,
           dau.user_id,
           up.RESELLER_ACTIVE_PHASE_LEVEL_1,
           up.RESELLER_ACTIVE_PHASE_LEVEL_2,
           ACQUISITION_PHASE
    FROM centralized_monitoring dau
    JOIN user_phase_detail up 
        ON dau.user_id = up.user_id 
        AND DATE_TRUNC('month', dau.period) = up.month_at
    WHERE 1=1
    -- QUALIFY ROW_NUMBER() OVER (PARTITION BY dau.user_id ORDER BY dau.event_at) = 1
),
dau_dxx AS (
    
    SELECT dau.user_id,
           DATE_TRUNC('day', dau.period) AS event_date,
           ds.start_date,
           DATEDIFF('day', ds.start_date, DATE_TRUNC('day', dau.period)) AS days_to_dx,
           'D' || LPAD(DATEDIFF('day', ds.start_date, DATE_TRUNC('day', dau.period))::VARCHAR, 2, '0') AS d_type
    FROM centralized_monitoring dau
    JOIN dau_d00 ds ON dau.user_id = ds.user_id
    WHERE dau.period >= ds.start_date
      AND DATEDIFF('day', ds.start_date, DATE_TRUNC('day', dau.period)) <= 30
),
summary_user_dxx AS (
    
    SELECT
        start_date AS period_start,
        d_type,
        COUNT(DISTINCT user_id) AS total_user
    FROM dau_dxx
    GROUP BY ALL
),
summary_user_all AS (
    
    SELECT
        start_date AS period_start,
        'Total Reseller' AS d_type,
        COUNT(DISTINCT user_id) AS total_user
    FROM dau_d00
    GROUP BY ALL
),
final AS (
    
    SELECT * FROM summary_user_all 
    UNION ALL 
    SELECT * FROM summary_user_dxx
)


SELECT pvt.*
FROM final
PIVOT
( 
    SUM(total_user) FOR d_type IN (
    'D00','D01','D02','D03','D04','D05','D06','D07','D08','D09','D10',
    'D11','D12','D13','D14','D15','D16','D17','D18','D19','D20',
    'D21','D22','D23','D24','D25','D26','D27','D28','D29','D30'
    )
) AS pvt (
    period_start,
    D00,D01,D02,D03,D04,D05,D06,D07,D08,D09,D10,
    D11,D12,D13,D14,D15,D16,D17,D18,D19,D20,
    D21,D22,D23,D24,D25,D26,D27,D28,D29,D30
)
ORDER BY period_start ASC
