const appDB = App.mysql.db;

export interface VisitorInfo {
  ip: string;
  ts: Date;
  count: number;
  host: string;
  countryName: string;
  subdivision1: string;
  subdivision2: string;
  cityName: string;
}

export async function getRecentVisitors(
  start: Date,
  end: Date,
  limit: number,
  host: string
): Promise<VisitorInfo[]> {
  const query = `
with
  ipt as (
    select ts, host, substring_index(remote_addr, ':', 1) as ip
    from caddy_log.logs
    where ts between :start and :end
    ${host ? "and host = :host" : ""}
  ),
  unique_ipt as (
    select max(ip) as ip, max(ts) as ts, count(*) as cnt, host
      from ipt group by ipt.ip, ipt.host
  ),
  ipgeo as (select ts, ip, cnt, host, geoip.query_city_geoname_id_by_ipv4(ip) as geoid from unique_ipt)
  select /*+ NO_MERGE(ipgeo) */ ipgeo.ip, ipgeo.ts, ipgeo.cnt, ipgeo.host,
    iploc.country_name, iploc.subdivision_1_name, iploc.subdivision_2_name, iploc.city_name from ipgeo
    left join geoip.geoip_city_locations as iploc on iploc.geoname_id = ipgeo.geoid and iploc.locale_code = 'zh-CN'
    order by ipgeo.ts desc
    limit :limit;
  `;
  const rows = await appDB.exec(
    query,
    {
      start: ["d", start],
      end: ["d", end],
      limit: ["i", limit],
      host: ["s", host],
    },
    "sdisssss"
  );
  return rows.map(([ip, ts, count, host, countryName, subdivision1, subdivision2, cityName]) => ({
    ip: ip!,
    ts: ts!,
    count: count!,
    host: host!,
    countryName: countryName!,
    subdivision1: subdivision1!,
    subdivision2: subdivision2!,
    cityName: cityName!,
  }));
}

export interface RequestTrace {
  caddy: CaddyLogEntry[];
  blueboat: BlueboatLogEntry[];
}

export interface CaddyLogEntry {
  ts: Date;
  userId: string;
  duration: number;
  size: number;
  status: number;
  respHeaders: Record<string, string[]>;
  remoteAddr: string;
  proto: string;
  method: string;
  host: string;
  uri: string;
  reqHeaders: Record<string, string[]>;
}

export interface BlueboatLogEntry {
  apppath: string;
  appversion: string;
  reqid: string;
  msg: string;
  logseq: number;
  logtime: Date;
}

const reqidMatcher = /^u:([0-9a-z-]+)$/;

export async function getRequestTrace(
  requestId: string
): Promise<RequestTrace> {
  if (!reqidMatcher.test(requestId))
    throw new Error("bad request id: " + requestId);
  const self_id: ["s", string] = ["s", requestId];
  const prefix: ["s", string] = ["s", requestId + "+%"];

  const caddyQuery = `
  select ts, user_id, duration, size, status_code, resp_headers,
          remote_addr, proto, method, host, uri, req_headers
      from caddy_log.logs
      where analytics_blueboat_request_id = :self_id or analytics_blueboat_request_id like :prefix
      order by ts asc
  `;
  const bbQuery = `
  select apppath, appversion, reqid, msg, logseq, logtime
      from rwv2.applog
      where reqid = :self_id or reqid like :prefix
      order by logtime asc
  `;

  const caddyRes = await appDB.exec(
    caddyQuery,
    {
      self_id,
      prefix,
    },
    "dsfiisssssss"
  );
  const bbRes = await appDB.exec(
    bbQuery,
    {
      self_id,
      prefix,
    },
    "ssssid"
  );
  return {
    caddy: caddyRes.map(
      ([
        ts,
        userId,
        duration,
        size,
        status,
        respHeaders,
        remoteAddr,
        proto,
        method,
        host,
        uri,
        reqHeaders,
      ]) => ({
        ts: ts!,
        userId: userId!,
        duration: duration!,
        size: size!,
        status: status!,
        respHeaders: JSON.parse(respHeaders!),
        remoteAddr: remoteAddr!,
        proto: proto!,
        method: method!,
        host: host!,
        uri: uri!,
        reqHeaders: JSON.parse(reqHeaders!),
      })
    ),
    blueboat: bbRes.map(
      ([apppath, appversion, reqid, msg, logseq, logtime]) => ({
        apppath: apppath!,
        appversion: appversion!,
        reqid: reqid!,
        msg: msg!,
        logseq: logseq!,
        logtime: logtime!,
      })
    ),
  };
}
