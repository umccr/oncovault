{% macro generate_hash_diff(columns) %}
    lower(to_hex(sha256(
        cast(concat(
            {% for col in columns %}
                cast({{ col }} as varchar)
                {% if not loop.last %},{% endif %}
            {% endfor %}
        ) as varbinary)
    )))
{% endmacro %}
