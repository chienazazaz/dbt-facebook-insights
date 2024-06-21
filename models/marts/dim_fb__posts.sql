WITH extracted_fields AS (
    SELECT
        *
    EXCEPT(body_content),
        SPLIT(body_content,"\n") [safe_offset(0)] AS tag_line,
        SPLIT(body_content,"-----------") [safe_offset(0)] AS main_body,
        SPLIT(body_content,"-----------") [safe_offset(2)] AS first_footer,
        SPLIT(body_content,"-----------") [safe_offset(4)] AS second_footer,
    FROM
        {{ ref("stg_fb__page_posts") }}
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['tag_line']) }} AS content_id,
FROM
    extracted_fields
