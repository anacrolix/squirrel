create trigger if not exists after_insert_blob_data
after insert on blob_data
begin
    update blob_meta set value=value+length(new.data) where key='size';
    delete from blob_data where data_id in (select blob_data_id from deletable_blob);
end;

create trigger if not exists after_update_blob_data
after update of data on blob_data
begin
    update blob_meta set value=value+length(new.data)-length(old.data) where key='size';
    delete from blob_data where data_id in (select blob_data_id from deletable_blob);
end;

create trigger if not exists after_delete_blob_data
after delete on blob_data
begin
    update blob_meta set value=value-length(old.data) where key='size';
end;
