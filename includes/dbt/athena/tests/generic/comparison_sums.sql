{% test multi_model_test(model, column_name, other_model, other_column, id_left, id_right) %}
SELECT *
FROM (
    SELECT SUM({{ model }}.{{ column_name }}) - SUM({{ other_model }}.{{ other_column }}) AS diff
    FROM {{ model }}
    LEFT JOIN {{ other_model }}
        ON {{ model }}.{{ id_left }} = {{ other_model }}.{{ id_right }}
) AS sums
WHERE sums.diff != 0
{% endtest %}