/// <reference path="../node_modules/blueboat-types/src/index.d.ts" />

import { getRecentVisitors, getRequestTrace } from "./query";

Router.get("/", () => new Response("ok"));
Router.get("/api/visitors", async (req) => {
  const u = new URL(req.url);
  const start = new Date(u.searchParams.get("start") || "");
  const end = new Date(u.searchParams.get("end") || Date.now());
  const limit = parseInt(u.searchParams.get("limit") || "");
  const host = u.searchParams.get("host") || "";
  return mkJsonResponse(await getRecentVisitors(start, end, limit, host));
});
Router.get("/api/request", async (req) => {
  const u = new URL(req.url);
  const id = u.searchParams.get("id") || "";
  return mkJsonResponse(await getRequestTrace(id));
});

function mkJsonResponse(x: unknown, status: number = 200): Response {
  return new Response(JSON.stringify(x, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}
