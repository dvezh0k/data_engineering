/* Создание таблиц */
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

/* Создание функции обновления поля updated_at в таблице users */
create or replace function replace_updated_at()
returns trigger as $$
begin
	update users
	set updated_at = now()
	where id = new.id;
	
	return new;
end;
$$ language plpgsql;

/* Создание триггера на обновление поля updated_at после обновления записей в users */
create or replace trigger trigger_replace_updated_at
after update on users
for each row
when (old.updated_at is not distinct from new.updated_at)
execute function replace_updated_at();

/* Создание функции логирования изменений */
create or replace function log_user_info_update()
returns trigger as $$
declare
	field_name text;
	old_value text;
	new_value text;
begin
	for field_name, new_value in
	select key, value::text from jsonb_each_text(to_jsonb(new))
	loop
		old_value := (to_jsonb(old) ->> field_name)::text;
		if new_value is distinct from old_value and field_name <> 'updated_at' then
			insert into users_audit(user_id, changed_by, field_changed, old_value, new_value)
			values (old.id, current_user, field_name, old_value, new_value);
		end if;
	end loop;

	return new;
end;
$$ language plpgsql;

/* Создание триггера на функцию логирования изменений */
create or replace trigger trigger_log_user_info_update
before update on users
for each row
execute function log_user_info_update();

/* Установка расширения pg_cron */
create extension if not exists pg_cron;

/* 
Создание функции экспорта изменений в csv файл
Поскольку дефолтной timezone для БД было установлено Europe/Moscow,
изменения будут браться за период с момента вызова - 1 день по момент вызова функции в timezone Europe/Moscow.
Запуск функции будет производиться на следующие сутки,
поэтому для получения корректной даты, за которую сформирован отчет, необходимо вычесть определенный интервал.
*/
create or replace function export_daily_user_update_log()
returns void as $$
declare
	filename text;
	begin
		filename := '/tmp/users_audit_export_' || to_char(now() - interval '6 hours', 'YYYY-MM-DD') || '.csv';
		execute format(
			'COPY (
				SELECT user_id, field_changed, old_value, new_value, changed_by, changed_at
				FROM users_audit
				WHERE changed_at < now() and changed_at > now() - interval ''1 day'')
			TO %L WITH CSV HEADER DELIMITER '','';', filename
);
	end;
$$ language plpgsql security definer;

/*
Настройка автозапуска для планировщика
Поскольку дефолтная timezone для БД - Europe/Moscow,
а дефолтная timezone для cron - GMT,
При указании периода запуска требуется вычесть 3 часа для соответствия ТЗ
(03:00 по Europe/Moscow = 00:00 по GMT)
При этом следуюет помнить, что функция export_daily_user_update_log работает в timezone Europe/Moscow

P.S. Полагаю, что в production среде можно было бы привести timezone БД и cron
к единому формате через настройку postgresql.conf, но в таком случае проверка
выполнения текущего ТЗ кажется неосуществимой
*/
select cron.schedule(
	'export-daily-user-audit',
	'0 0 * * *',
	$$ select export_daily_user_update_log() $$
);

/* Команды для проверки*/
insert into users (name, email, role)
values 
('Ivan Ivanov', 'ivan@example.com', 'developer'),
('Anna Petrova', 'anna@example.com', 'seller');

update users
set 
	email = 'ivan.new@example.com',
	role = 'lead developer'
where name = 'Ivan Ivanov';

update users
set 
	role = 'junior developer'
where name = 'Anna Petrova';

select * from users_audit;

select export_daily_user_update_log()

select * from cron.job;
