

{% set my_cool_string = 'woow cool' %}
{% set my_second_cool_string = 'this is jinja' %}
{{my_cool_string}} {{my_second_cool_string}}

----------

{% set my_animals = ['lemur', 'wolf', 'panther', 'tardigrade'] %}
{{ my_animals [0] }}
{{ my_animals [1] }}
{{ my_animals [2] }}
{{ my_animals [3] }}

------------

{% set my_animals = ['lemur', 'wolf', 'panther', 'tardigrade'] %}
{{ my_animals [0] }}

{% for animal in my_animals %}
    My favorite animal is the {{ animal }}
{% endfor %}

----

{% set temperature = 20 %}
{% if temperature < 65 %}
    Time for a cappuccino!
{% else %} :
Time for a cold brew!
{% endif %}

-------

{% set websters_dict = {
'word' : 'data',
'speech_part' : 'noun',
'definition': 'if you know you know',
} %}

{{websters_dict['word']}} {{websters_dict['speech_part']}}