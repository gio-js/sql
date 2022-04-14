/*
20220410: g.sorgente, created

example:
exec SpUnusedIndexes @dbName='DB',@type='unused';
exec SpUnusedIndexes @dbName='DB',@type='writecost';
*/
CREATE OR alter PROCEDURE [dbo].[SpUnusedIndexes] (
	@dbName sysname = null,
	@type sysname = null
)
as
begin
set nocount on;
if @dbName is null or @type is null
	begin
	print 'Stored procedure SpUnusedIndexes usage:';
	print ' (no param specified)	    -- print this help';
	print ' @dbName=null || @type=null	-- print this help';
	print ' @dbName=''my_database''		-- database on which execute the analysis';
	print ' @type=''unused''             -- gets all the indexes not affected by any write nor reads';
	print ' @type=''writecost''		    -- compares all the write and read operations on indexes and returns ';
	print '						   the ones who does have too high write costs compared to usage on reads';
	goto ret;
	end

-- set a non blocking isolation level
set transaction isolation level read uncommitted;

-- decls
declare @database sysname, @db_id int, @role sysname, @read_write_cost float, @sql nvarchar(max);
select top 1 
	  @role=(select case when sys.fn_hadr_backup_is_preferred_replica( db_name() ) = 1 then 'primary' else 'replica' end)
	, @read_write_cost = 0.01 -- 1%
	, @database = quotename(@dbName)
	, @db_id = db_id(@dbName)

if @read_write_cost < 0 or @read_write_cost > 1 
	begin
    print 'Please specify a read/write cost threshold between 0 and 1';
	goto ret;
    end

if @type='unused' goto unused_indexes;
if @type='writecost' goto writecost;
goto ret;

unused_indexes:
-- indexes without a reference on usage stats

set @sql = N'
use '+@database+';

select 
	@type as report_type,
	i.object_id as index_object_id,
	s.name as schema_name,
	t.name as table_name,
	i.name as index_name,
	ps.partition_number,
    ps.row_count,
    Cast((ps.reserved_page_count * 8)/1024. as decimal(12,2)) as size_in_mb,
	iu.user_seeks,
	iu.user_scans,
	iu.user_lookups,
	iu.user_updates,
	iu.system_seeks,
	iu.system_scans,
	iu.system_lookups,
	iu.system_updates
from 
	sys.indexes i 
	join sys.tables t  on i.object_id = t.object_id and t.type = ''U''
	join sys.schemas s  on s.schema_id = t.schema_id
	join sys.dm_db_partition_stats ps ON i.object_id = ps.object_id AND i.index_id = ps.index_id
	left join sys.dm_db_index_usage_stats iu on iu.database_id = @db_id and i.object_id = iu.object_id and i.index_id = iu.index_id
where 
	i.is_unique = 0 -- unique indexes cannot be deleted
	and i.is_primary_key != 1
	and i.type != 0 -- heap
	and isnull(iu.user_seeks,0) = 0
	and isnull(iu.user_scans,0) = 0
	and isnull(iu.user_lookups,0) = 0
	and isnull(iu.user_updates,0) = 0

	and isnull(iu.system_seeks,0) = 0
	and isnull(iu.system_scans,0) = 0
	and isnull(iu.system_lookups,0) = 0
	and isnull(iu.system_updates,0) = 0
group by
	i.object_id,
	s.name,
	t.name,
	i.name,
	ps.partition_number,
    ps.row_count,
    ps.reserved_page_count,
	iu.user_seeks,
	iu.user_scans,
	iu.user_lookups,
	iu.user_updates,
	iu.system_seeks,
	iu.system_scans,
	iu.system_lookups,
	iu.system_updates
order by 
	8 desc
';

exec sp_executesql @sql, N'@db_id int, @type sysname', @db_id, @type;

goto ret;

writecost:
-- indexes affected by updates and low reads
set @sql = N'
use '+@database+';

select 
	@type as report_type,
	i.object_id as index_object_id,
	s.name as schema_name,
	t.name as table_name,
	i.name as index_name,
	ps.partition_number,
    ps.row_count,
    Cast((ps.reserved_page_count * 8)/1024. as decimal(12,2)) as size_in_mb,
	iu.user_seeks,
	iu.user_scans,
	iu.user_lookups,
	iu.user_updates,
	iu.system_seeks,
	iu.system_scans,
	iu.system_lookups,
	iu.system_updates,
	iu.system_updates * @read_write_cost as system_updates_threshold,
	iu.user_updates * @read_write_cost as user_updates_threshold,
	isnull(iu.user_seeks,0) * @read_write_cost     as user_seeks_cost,
	isnull(iu.user_scans,0) * @read_write_cost     as user_scans_cost,
	isnull(iu.user_lookups,0) * @read_write_cost   as user_lookups_cost,
	isnull(iu.system_seeks,0) * @read_write_cost   as system_seeks_cost,
	isnull(iu.system_scans,0) * @read_write_cost   as system_scans_cost,
	isnull(iu.system_lookups,0) * @read_write_cost as system_lookups_cost
from 
	sys.indexes i 
	join sys.tables t  on i.object_id = t.object_id and t.type = ''U''
	join sys.schemas s  on s.schema_id = t.schema_id
	join sys.dm_db_partition_stats ps ON i.object_id = ps.object_id AND i.index_id = ps.index_id
	left join sys.dm_db_index_usage_stats iu (nolock) on iu.database_id = @db_id and i.object_id = iu.object_id and i.index_id = iu.index_id
where 
	i.is_unique = 0 -- unique indexes cannot be deleted
	and i.is_primary_key != 1
	and i.type != 0 -- heap
	and (
		isnull(iu.user_updates,0) > 0 and (
			isnull(iu.user_seeks,0) <= isnull(iu.user_updates,0) * @read_write_cost
			and isnull(iu.user_scans,0) <= isnull(iu.user_updates,0) * @read_write_cost
			and isnull(iu.user_lookups,0) <= isnull(iu.user_updates,0) * @read_write_cost
		)
		or isnull(iu.system_updates,0) > 0 and (
			isnull(iu.system_seeks,0) <= isnull(iu.system_updates,0) * @read_write_cost
			and isnull(iu.system_scans,0) = isnull(iu.system_updates,0) * @read_write_cost
			and isnull(iu.system_lookups,0) <= isnull(iu.system_updates,0) * @read_write_cost
		)
	)
group by
	i.object_id,
	s.name,
	t.name,
	i.name,
	ps.partition_number,
    ps.row_count,
    ps.reserved_page_count,
	iu.user_seeks,
	iu.user_scans,
	iu.user_lookups,
	iu.user_updates,
	iu.system_seeks,
	iu.system_scans,
	iu.system_lookups,
	iu.system_updates
order by 
	8 desc
';
exec sp_executesql @sql, N'@db_id int, @type sysname, @read_write_cost float', @db_id, @type, @read_write_cost;

goto ret;

ret:
print 'end';

end -- end proc
