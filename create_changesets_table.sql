create table changesets (
	id bigint,
	created_at timestamp without time zone,
	closed_at timestamp without time zone,
	is_open boolean,
	"user" varchar,
	uid int,
	min_lat float,
	max_lat float,
	min_lon float,
	max_lon float,
	comments_count int,
	num_changes int);