package mayton.watchdog.pg

/*
 TODO: Should be investigated for next optional or mandatory values

 SELECT column_name, data_type, is_nullable, character_maximum_length
 FROM
        information_schema.columns
 WHERE
        table_schema = current_schema()
        AND table_name = 'geoipcity'
        ORDER BY ordinal_position;

 */


case class ColumnDefinition( val columnName : String,
                             val dataType   : String,
                             val characterMaximumLength : Int,
                             val isNullable : Boolean)
