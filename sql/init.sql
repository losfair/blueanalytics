grant select on caddy_log.logs to blueanalytics;
grant select on geoip.* to blueanalytics;
grant select on rwv2.* to blueanalytics;
grant execute on function geoip.query_city_geoname_id_by_ipv4 to blueanalytics;

alter table caddy_log.logs add column analytics_blueboat_request_id varchar(255) as (
  resp_headers->>'$."X-Blueboat-Request-Id"[0]'
) stored;

create index analytics_by_blueboat_request_id on caddy_log.logs (analytics_blueboat_request_id);
