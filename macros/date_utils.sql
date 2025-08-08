{% macro weekday_or_weekend(x) %}
CASE
    WHEN DAYNAME(TO_TIMESTAMP({{x}})) IN ('Sat', 'Sun') THEN 'WEEKEND'
    ELSE 'WEEKDAY'
END
{% endmacro %}

{% macro season(x) %}
CASE 
    WHEN MONTH(TO_TIMESTAMP({{x}})) IN (12, 1, 2) THEN 'WINTER'
    WHEN MONTH(TO_TIMESTAMP({{x}})) IN (3, 4, 5) THEN 'SPRING'
    WHEN MONTH(TO_TIMESTAMP({{x}})) IN (6, 7, 8) THEN 'SUMMER'
    WHEN MONTH(TO_TIMESTAMP({{x}})) IN (9, 10, 11) THEN 'SUMMER'
END
{% endmacro %}
