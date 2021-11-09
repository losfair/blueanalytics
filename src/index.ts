/// <reference path="../node_modules/blueboat-types/src/index.d.ts" />

import { getRequestTrace } from "./query";

Router.get("/", () => new Response("ok"));
Router.get("/api/request", async (req) => {
  const u = new URL(req.url);
  const t = parseInt(u.searchParams.get("t") || "");
  const id = u.searchParams.get("id") || "";
  return mkJsonResponse(await getRequestTrace(t, id));
});

function mkJsonResponse(x: unknown, status: number = 200): Response {
  return new Response(JSON.stringify(x, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}
