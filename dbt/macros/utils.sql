{% macro generate_surrogate_key(field_list) %}
    {# Generate a surrogate key using MD5 hash #}
    MD5(CONCAT(
        {% for field in field_list %}
            COALESCE(CAST({{ field }} AS STRING), '_NULL_')
            {% if not loop.last %}, '|', {% endif %}
        {% endfor %}
    ))
{% endmacro %}


{% macro get_date_range(start_date, end_date) %}
    {# Generate a date range for filtering #}
    event_date >= '{{ start_date }}' AND event_date <= '{{ end_date }}'
{% endmacro %}


{% macro cents_to_dollars(column_name, precision=2) %}
    {# Convert cents to dollars #}
    ROUND({{ column_name }} / 100, {{ precision }})
{% endmacro %}


{% macro calculate_conversion_rate(numerator, denominator, precision=4) %}
    {# Calculate conversion rate safely #}
    CASE 
        WHEN {{ denominator }} > 0 
        THEN ROUND({{ numerator }} * 100.0 / {{ denominator }}, {{ precision }})
        ELSE 0 
    END
{% endmacro %}


{% macro get_rfm_segment(r_score, f_score, m_score) %}
    {# Assign RFM segment based on scores #}
    CASE
        WHEN {{ r_score }} >= 4 AND {{ f_score }} >= 4 AND {{ m_score }} >= 4 THEN 'Champions'
        WHEN {{ r_score }} >= 3 AND {{ f_score }} >= 3 AND {{ m_score }} >= 3 THEN 'Loyal Customers'
        WHEN {{ r_score }} >= 4 AND {{ f_score }} <= 2 THEN 'New Customers'
        WHEN {{ r_score }} <= 2 AND {{ f_score }} >= 4 THEN 'At Risk'
        WHEN {{ r_score }} <= 2 AND {{ f_score }} <= 2 THEN 'Lost'
        ELSE 'Others'
    END
{% endmacro %}
