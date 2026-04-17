/* Создание таблицы сырых логов событий */
create table user_events (
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
) engine = MergeTree()
order by (event_time, user_id)
TTL event_time + interval 30 day delete;

/* Создание агрегированной таблицы логов событий */
create table user_events_aggregated (
	event_type String,
	event_date Date,
	unique_users_state AggregateFunction(uniq, UInt32),
	points_spent_sum_state AggregateFunction(sum, UInt32),
	events_qty_state AggregateFunction(count, UInt32)
) engine = AggregatingMergeTree()
order by (event_type, event_date)
TTL event_date + interval 180 day delete;

/* Создание материализованного представления для обновления агрегированной таблицы */
create materialized view mv_user_events_aggregated
to user_events_aggregated as
select
	event_type,
	toDate(event_time) as event_date,
	uniqState(user_id) as unique_users_state,
	sumState(points_spent) as points_spent_sum_state,
	countState() as events_qty_state
from user_events
group by event_type, event_date

/* Заполнение таблиц тестовым датасетом */
INSERT INTO user_events VALUES
	(1, 'login', 0, now() - INTERVAL 10 DAY),
	(2, 'signup', 0, now() - INTERVAL 10 DAY),
	(3, 'login', 0, now() - INTERVAL 10 DAY),
	(1, 'login', 0, now() - INTERVAL 7 DAY),
	(2, 'login', 0, now() - INTERVAL 7 DAY),
	(3, 'purchase', 30, now() - INTERVAL 7 DAY),
	(1, 'purchase', 50, now() - INTERVAL 5 DAY),
	(2, 'logout', 0, now() - INTERVAL 5 DAY),
	(4, 'login', 0, now() - INTERVAL 5 DAY),
	(1, 'login', 0, now() - INTERVAL 3 DAY),
	(3, 'purchase', 70, now() - INTERVAL 3 DAY),
	(5, 'signup', 0, now() - INTERVAL 3 DAY),
	(2, 'purchase', 20, now() - INTERVAL 1 DAY),
	(4, 'logout', 0, now() - INTERVAL 1 DAY),
	(5, 'login', 0, now() - INTERVAL 1 DAY),
	(1, 'purchase', 25, now()),
	(2, 'login', 0, now()),
	(3, 'logout', 0, now()),
	(6, 'signup', 0, now()),
	(6, 'purchase', 100, now());

/* Расчет Retention */
with 
ordered_users_visit_history as (
	select
		user_id,
		toDate(event_time) as visit_date,
		event_type,
		rank() over (
			partition by user_id
			order by toDate(event_time)
		) as visit_day_number
	from user_events
	order by user_id, visit_date
),
first_and_second_visit_dates as (
	select
		first_visit.user_id,
		min(first_visit.visit_date) as first_visit_date,
		max(second_visit.visit_date) as second_visit_date
	from ordered_users_visit_history as first_visit
	left join ordered_users_visit_history as second_visit
		on first_visit.user_id = second_visit.user_id
		and first_visit.visit_day_number = 1
		and second_visit.visit_day_number = 2
	group by first_visit.user_id 
	order by first_visit.user_id
)

select
	count(user_id) as total_users_day_0,
	sum(
		case
			when first_visit_date >= second_visit_date - interval 7 day then 1
			else 0
		end	
	) as returned_in_7_days,
	round(returned_in_7_days / total_users_day_0 * 100, 2) as retention_7d_percent
from first_and_second_visit_dates

/* Запрос с группировками для быстрой аналитики по дням */
select
	event_date,
	event_type,
	uniqMerge(unique_users_state) as unique_users,
	sumMerge(points_spent_sum_state) as total_spent,
	countMerge(events_qty_state) as total_actions
from user_events_aggregated
group by event_date, event_type
order by event_date, event_type
