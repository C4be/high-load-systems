select string_agg(column_name, ', ') from information_schema."columns" c 
where c.table_schema = 'public'
and c.table_name = 'mock_data';

select column_name
from information_schema."columns" c 
where c.table_schema = 'public'
and c.table_name = 'mock_data';