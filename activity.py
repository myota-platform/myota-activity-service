from __future__ import annotations

from http.server import ThreadingHTTPServer
from typing import Any

from common import JsonHandler, Store, new_id, now, page_result, require, verify_token
from awards import AwardsHandler


class ActivityHandler(JsonHandler):
    service = "activity-service"
    store = Store("activity", "CORE_DATABASE_URL")

    @staticmethod
    def list_activations(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        if p.get("_http"):
            authorization = p.get("Authorization", "")
            if not authorization.startswith("Bearer "):
                raise PermissionError("Bearer authentication is required")
            claims = verify_token(authorization[7:])
            scopes = set(claims.get("scp", []))
            if not {"*", "activity.read", "activity.admin"}.intersection(scopes):
                raise PermissionError("activity read scope is required")
        from urllib.parse import parse_qs, urlparse
        query = parse_qs(urlparse(p.get("_path", "")).query)
        items = list(ActivityHandler.store.items.values())
        if query.get("programme"):
            items = [a for a in items if a.get("programmeSlug") == query["programme"][0]]
        return page_result(items, query)

    @staticmethod
    def create_activation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "programmeSlug", "entityId", "operatorId", "startedAt")
        def create() -> dict[str, Any]:
            activation = {"id": new_id(), "programmeSlug": body["programmeSlug"], "entityId": body["entityId"],
                          "operatorId": body["operatorId"], "startedAt": body["startedAt"], "endedAt": body.get("endedAt"),
                          "entityType": body.get("entityType"), "location": body.get("location"),
                          "status": "OPEN", "qsos": [], "createdAt": now(), "updatedAt": now()}
            ActivityHandler.store.items[activation["id"]] = activation
            ActivityHandler.store.event("activity.activation.created.v1", "activation", activation["id"], activation)
            return {**activation, "_status": 201}
        return ActivityHandler.store.once(p.get("Idempotency-Key"), create)

    @staticmethod
    def get_activation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        return ActivityHandler.store.items[p["activationId"]]

    @staticmethod
    def add_qso(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "workedCallsign", "timestamp")
        activation = ActivityHandler.store.items[p["activationId"]]
        if activation["status"] != "OPEN":
            raise ValueError("activation is not open")
        def add() -> dict[str, Any]:
            qso = {"id": new_id(), "workedCallsign": body["workedCallsign"].upper(), "timestamp": body["timestamp"],
                   "band": body.get("band"), "mode": body.get("mode"), "rst": body.get("rst"),
                   "hunterId": body.get("hunterId"), "workedEntityId": body.get("workedEntityId"),
                   "source": body.get("source", "manual"), "createdAt": now()}
            activation["qsos"].append(qso)
            activation["updatedAt"] = now()
            ActivityHandler.store.event("activity.qso.recorded.v1", "activation", activation["id"], qso)
            return {"activationId": activation["id"], "qso": qso, "qsoCount": len(activation["qsos"]), "_status": 201}
        return ActivityHandler.store.once(p.get("Idempotency-Key"), add)

    @staticmethod
    def close_activation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        activation = ActivityHandler.store.items[p["activationId"]]
        activation["status"] = "CLOSED"
        activation["endedAt"] = p["_body"].get("endedAt", now())
        activation["updatedAt"] = now()
        ActivityHandler.store.event("activity.activation.closed.v1", "activation", activation["id"], activation)
        return activation


ActivityHandler.routes = {
    ("GET", "/v1/activations"): ActivityHandler.list_activations,
    ("POST", "/v1/activations"): ActivityHandler.create_activation,
    ("GET", "/v1/activations/{activationId}"): ActivityHandler.get_activation,
    ("POST", "/v1/activations/{activationId}/qsos"): ActivityHandler.add_qso,
    ("POST", "/v1/activations/{activationId}/close"): ActivityHandler.close_activation,
    **AwardsHandler.routes,
}

# Activity and award execution share one bounded service and one durable
# service-state/outbox stream. Awards still keep their own named buckets and
# API paths, but are intentionally exposed through this process on port 8004.
AwardsHandler.store = ActivityHandler.store


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8004), ActivityHandler).serve_forever()
