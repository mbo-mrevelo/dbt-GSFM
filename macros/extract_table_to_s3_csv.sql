{% macro extract_table_to_s3_csv(s3_stage,file_format) %}

    {% set qualified_relation_name = this.database + '.' + this.schema + '.' + this.table %}

    {% set s3_file_path = this.table + '_' + modules.datetime.datetime.utcnow().strftime("%Y-%m-%d") + '.csv' %}  --.gz removed for now
    
    copy into {{ s3_stage }}/{{ s3_file_path }}
    from {{ qualified_relation_name }}
    file_format = {{ file_format }}
    single = true
    header = true
    overwrite = true
    max_file_size = 1073741824;

{% endmacro %}