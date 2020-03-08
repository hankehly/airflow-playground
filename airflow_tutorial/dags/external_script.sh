{% for i in range(5) %}
    echo "external script template"
    echo "{{ params.my_other_param }}"
    echo "{{ 'world' | hello }}"
{% endfor %}
