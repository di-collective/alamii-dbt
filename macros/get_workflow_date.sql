{% macro get_workflow_date() %}
  {% if var('date') == 'Yesterday' %}
    date_sub(current_date("Asia/Jakarta"), INTERVAL 1 day)
  {% elif var('date') == 'H-2' %}
    date_sub(current_date("Asia/Jakarta"), INTERVAL 2 day)
  {% elif var('date') == 'H-3' %}
    date_sub(current_date("Asia/Jakarta"), INTERVAL 3 day)
  {% elif var('date') == 'H-4' %}
    date_sub(current_date("Asia/Jakarta"), INTERVAL 4 day)
  {% elif var('date') == 'H-5' %}
    date_sub(current_date("Asia/Jakarta"), INTERVAL 5 day)
  {% elif var('date') == 'H-6' %}
    date_sub(current_date("Asia/Jakarta"), INTERVAL 6 day)
  {% elif var('date') == 'H-7' %}
    date_sub(current_date("Asia/Jakarta"), INTERVAL 7 day)
  {% else %}
    current_date("Asia/Jakarta")
  {% endif %}
{% endmacro %}