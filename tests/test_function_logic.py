import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from backend.conductor_common import TENANT_RESUME_DRIVER_PATH, TENANT_START_NEXT_PATH  # noqa: E402
from backend.function_logic import ACTOR_LAMBDA, FunctionBackend  # noqa: E402
from chask_foundation.backend.models import OrchestrationEvent  # noqa: E402


EVENT_ID = "11111111-2222-4333-8444-555555555555"
SESSION_ID = "22222222-3333-4444-8555-666666666666"
ROUTE_STOP_ID = "aaaaaaaa-1111-4111-8111-111111111111"
PICKUP_ORDER_ID = "bbbbbbbb-2222-4222-8222-222222222222"


def _event(args=None):
    return OrchestrationEvent.model_validate(
        {
            "event_id": EVENT_ID,
            "event_type": "function_call",
            "branch": "test",
            "organization_customer_id": None,
            "customer": None,
            "connection_key": "test",
            "organization": {
                "organization_id": "99999999-aaaa-4bbb-8ccc-dddddddddddd",
                "organization_name": "Chask Dev",
            },
            "prompt": "",
            "pipeline_id": 45681,
            "orchestration_session_uuid": SESSION_ID,
            "internal_orchestration_session_uuid": None,
            "channel_id": None,
            "entry_point_channel": "whatsapp",
            "source": "agent",
            "target": "function",
            "plan": None,
            "extra_params": {
                "user_phone_number": "+56 9 1111 2222",
                "agent_phone_number": "1051240901403291",
                "tool_calls": [{"args": args or {}}],
            },
            "access_token": "access-token",
            "target_agent": None,
            "target_operator": None,
            "type": None,
            "status": None,
            "channels": None,
            "whatsapp_template_instance": None,
            "created_at": None,
        }
    )


def _route_stop(**overrides):
    data = {
        "id": ROUTE_STOP_ID,
        "route_id": "cccccccc-3333-4333-8333-333333333333",
        "pickup_order_id": PICKUP_ORDER_ID,
        "stop_number": 2,
        "queue_position": 2,
        "clinic_name_snapshot": "Clinica Los Robles",
        "address_snapshot": "Av. Siempre Viva 123",
        "comuna_snapshot": "Providencia",
        "maps_url_snapshot": "https://maps.example/clinica",
        "route_code": "R-1",
    }
    data.update(overrides)
    return data


class FakeTenantClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, path, *, json=None):
        self.calls.append({"path": path, "json": json})
        return self.responses.pop(0)


class FakeOrchestrator:
    def __init__(self, history_events=None):
        self.calls = []
        self.history_events = history_events or []

    def call(self, endpoint, **kwargs):
        self.calls.append({"endpoint": endpoint, **kwargs})
        if endpoint == "get_orchestration_events":
            return {"orchestration_events": self.history_events}
        if endpoint == "evolve_event":
            return {
                "status_code": 201,
                "uuid": "77777777-2222-4222-8222-222222222222",
                "extra_params": kwargs["extra_params"],
            }
        return {"status_code": 200}


def test_reanudar_ruta_clears_pause_then_claims_explicit_route_stop(monkeypatch):
    tenant_client = FakeTenantClient(
        [
            {"driver": {"id": "dddddddd-4444-4444-8444-444444444444", "paused": False}},
            {"claimed": True, "route_stop": _route_stop(), "total_stops": 3},
        ]
    )
    fake_orchestrator = FakeOrchestrator()
    monkeypatch.setattr(
        "backend.conductor_common.ConductorContext.tenant_client",
        lambda self: tenant_client,
    )
    monkeypatch.setattr("backend.conductor_common.orchestrator_api_manager", fake_orchestrator)

    backend = FunctionBackend(
        _event({"route_stop_id": ROUTE_STOP_ID, "pickup_order_id": PICKUP_ORDER_ID})
    )
    result = backend.process_request()

    assert [call["path"] for call in tenant_client.calls] == [
        TENANT_RESUME_DRIVER_PATH,
        TENANT_START_NEXT_PATH,
    ]
    assert tenant_client.calls[1]["json"]["actor_lambda"] == ACTOR_LAMBDA
    assert tenant_client.calls[1]["json"]["action"] == "reanudar"
    assert tenant_client.calls[1]["json"]["route_stop_id"] == ROUTE_STOP_ID
    assert tenant_client.calls[1]["json"]["pickup_order_id"] == PICKUP_ORDER_ID
    assert "accion" not in tenant_client.calls[1]["json"]
    assert "Ruta R-1 reanudada" in result
    whatsapp_call = next(c for c in fake_orchestrator.calls if c.get("event_type") == "response_to_whatsapp_message")
    assert whatsapp_call["prompt"].startswith("Ruta reanudada. Siguiente parada")


def test_reanudar_ruta_missing_route_stop_uses_helper_guard_before_resume(monkeypatch):
    tenant_client = FakeTenantClient([])
    fake_orchestrator = FakeOrchestrator()
    monkeypatch.setattr(
        "backend.conductor_common.ConductorContext.tenant_client",
        lambda self: tenant_client,
    )
    monkeypatch.setattr("backend.conductor_common.orchestrator_api_manager", fake_orchestrator)

    result = FunctionBackend(_event({})).process_request()

    assert tenant_client.calls == []
    assert "route_stop_id" in result
    dispatch_call = next(c for c in fake_orchestrator.calls if c.get("event_type") == "dispatch_event")
    assert dispatch_call["extra_params"]["event_type"] == "conductor_route_stop_id_missing_terminal"
