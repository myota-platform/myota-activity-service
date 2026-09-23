from __future__ import annotations

from http.server import ThreadingHTTPServer
from typing import Any

from common import JsonHandler, Store, new_id, now, require


class ActivityHandler(JsonHandler):
    service = "activity-service"
    store = Store()

    @staticmethod
    def create_activation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "programmeSlug", "entityId", "operatorId", "startedAt")
        def create() -> dict[str, Any]:
            activation = {"id": new_id(), "programmeSlug": body["programmeSlug"], "entityId": body["entityId"],
                          "operatorId": body["operatorId"], "startedAt": body["startedAt"], "endedAt": body.get("endedAt"),
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
                   "band": body.get("band"), "mode": body.get("mode"), "rst": body.get("rst"), "source": body.get("source", "manual"), "createdAt": now()}
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
    ("POST", "/v1/activations"): ActivityHandler.create_activation,
    ("GET", "/v1/activations/{activationId}"): ActivityHandler.get_activation,
    ("POST", "/v1/activations/{activationId}/qsos"): ActivityHandler.add_qso,
    ("POST", "/v1/activations/{activationId}/close"): ActivityHandler.close_activation,
}


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8004), ActivityHandler).serve_forever()

