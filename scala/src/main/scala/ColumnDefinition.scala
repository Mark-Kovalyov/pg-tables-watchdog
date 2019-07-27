/*
 TODO: Should be investigated for next optional or mandatory values

 SELECT column_name, data_type, is_nullable, character_maximum_length
 FROM
        information_schema.columns
 WHERE
        table_schema = current_schema()
        AND table_name = 'geoipcity'
        ORDER BY ordinal_position;

 table_catalog | table_schema | table_name | column_name | ordinal_position | column_default | is_nullable | data_type |
 character_maximum_length | character_octet_length | numeric_precision | numeric_precision_radix | numeric_scale |
 datetime_precision | interval_type | interval_precision | character_set_catalog | character_set_schema |
 character_set_name | collation_catalog | collation_schema | collation_name | domain_catalog | domain_schema |
 domain_name | udt_catalog | udt_schema | udt_name | scope_catalog | scope_schema | scope_name | maximum_cardinality |
 dtd_identifier | is_self_referencing | is_identity | identity_generation | identity_start | identity_increment |
 identity_maximum | identity_minimum | identity_cycle | is_generated | generation_expression | is_updatable
 */


class ColumnDefinition( val columnName : String,
                        val dataType   : String,
                        val characterMaximumLength : Int,
                        val isNullable : Boolean)