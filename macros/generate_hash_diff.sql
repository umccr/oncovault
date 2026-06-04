{% macro generate_hash_diff(columns) %}
    lower(to_hex(sha256(
        cast(concat(
            {% for col in columns %}
                lower(to_hex(sha256(cast(
                    coalesce(cast({{ col }} as varchar), '')
                as varbinary))))
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        ) as varbinary)
    )))
{% endmacro %}
