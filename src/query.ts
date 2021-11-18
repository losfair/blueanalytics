const clickhouseQueryEndpoint = App.mustGetEnv("clickhouseQueryEndpoint");
const clickhouseUser = App.mustGetEnv("clickhouseUser");
const clickhousePassword = App.mustGetEnv("clickhousePassword");
const blueboatTables = App.mustGetEnv("blueboatTables").split(",");

export interface RequestTrace {
  caddy: CaddyLogEntry;
  blueboat: BlueboatLogEntry[];
}

export interface CaddyLogEntry {
  ts: Date;
  userId: string;
  duration: number;
  size: number;
  status: number;
  respHeaders: Record<string, string[]>;
  ip: string;
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
  time: number,
  requestId: string
): Promise<RequestTrace | null> {
  if (!reqidMatcher.test(requestId))
    throw new Error("bad request id: " + requestId);
  const caddyQueryGen = (tsUnsafe: number, reqidUnsafe: string) => {
    const tsB64 = Codec.b64encode(new TextEncoder().encode("" + tsUnsafe));
    const reqidB64 = Codec.b64encode(new TextEncoder().encode(reqidUnsafe));
    return `
  select
    ts, user_id, duration, cast(size as Float64) as size, status,
    resp_headers, ip, proto, method, host, uri, req_headers
    from caddy_analytics.logstream_requestinfo
    where ts >= date_sub(minute, 5, toDateTime64(cast(base64Decode('${tsB64}') as Float64) / 1000, 3))
      and ts <= date_add(minute, 5, toDateTime64(cast(base64Decode('${tsB64}') as Float64) / 1000, 3))
      and blueboat_reqid = base64Decode('${reqidB64}')
    limit 1
  `;
  };
  const bbQueryMetaGen =
    (tableName: string) =>
    (tsUnsafe: number, reqidUnsafe: string, prefixUnsafe: string) => {
      const tsB64 = Codec.b64encode(new TextEncoder().encode("" + tsUnsafe));
      const reqidB64 = Codec.b64encode(new TextEncoder().encode(reqidUnsafe));
      const prefixB64 = Codec.b64encode(new TextEncoder().encode(prefixUnsafe));
      return `
  select apppath, appversion, request_id, message, logseq, ts
    from blueboat_analytics.${tableName}
    where ts >= date_sub(minute, 5, toDateTime64(cast(base64Decode('${tsB64}') as Float64) / 1000, 3))
      and ts <= date_add(minute, 5, toDateTime64(cast(base64Decode('${tsB64}') as Float64) / 1000, 3))
      and (request_id = base64Decode('${reqidB64}') or request_id like base64Decode('${prefixB64}'))
    order by ts asc
  `;
    };
  const runBlueboatQuery = async (
    tsUnsafe: number,
    reqidUnsafe: string,
    prefixUnsafe: string
  ): Promise<any[]> => {
    const res: any[] = [];
    for(const tableName of blueboatTables) {
      res.push(
        ...(<any>(
          await queryClickhouse(
            bbQueryMetaGen(tableName)(
              tsUnsafe,
              reqidUnsafe,
              prefixUnsafe
            )
          )
        )).data
      );
    }
    return res;
  };

  const caddyRawRes = <any>(
    await queryClickhouse(caddyQueryGen(time, requestId))
  );
  if (!caddyRawRes.data.length) return null;
  const caddyRawData = caddyRawRes.data[0];

  const clEntry: CaddyLogEntry = {
    ts: new Date(caddyRawData.ts),
    userId: caddyRawData.user_id,
    duration: caddyRawData.duration,
    size: caddyRawData.size,
    status: caddyRawData.status,
    respHeaders: Object.fromEntries(caddyRawData.resp_headers),
    ip: caddyRawData.ip,
    proto: caddyRawData.proto,
    method: caddyRawData.method,
    host: caddyRawData.host,
    uri: caddyRawData.uri,
    reqHeaders: Object.fromEntries(caddyRawData.req_headers),
  };

  const bbRawDataList = await runBlueboatQuery(
    time,
    requestId,
    requestId + "+%"
  );
  const bbEntries: BlueboatLogEntry[] = bbRawDataList.map((x) => ({
    apppath: x.apppath,
    appversion: x.appversion,
    reqid: x.request_id,
    msg: x.message,
    logseq: x.logseq,
    logtime: new Date(x.ts),
  }));

  return {
    caddy: clEntry,
    blueboat: bbEntries,
  };
}

async function queryClickhouse(q: string): Promise<unknown> {
  const res = await fetch(clickhouseQueryEndpoint, {
    headers: {
      "X-ClickHouse-User": clickhouseUser,
      "X-ClickHouse-Key": clickhousePassword,
      "X-ClickHouse-Format": "JSON",
    },
    method: "POST",
    body: q,
  });
  if (res.status !== 200)
    throw new Error(
      `clickhouse returned non-200 status code (${
        res.status
      }): ${await res.text()}`
    );
  return await res.json();
}
